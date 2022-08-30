use std::collections::{hash_map::Entry, HashMap};
use std::error::Error;
use std::fs::File;
use std::path::Path;

use mysql::{params, prelude::*, Pool, TxOpts};
use osmpbfreader::{OsmObj, OsmPbfReader};

fn main() -> Result<(), Box<dyn Error>> {
    // Open the connection to the cwall directory DB.
    let conn_str = "mysql://cwall:cwall@localhost:3306/cwall_dir";
    let conn_pool = Pool::new(conn_str)?;
    let mut conn = conn_pool.get_conn()?;

    // Drop existing database tables.
    conn.query_drop("DROP TABLE IF EXISTS cwalls")?;

    // Now recreate them.
    conn.query_drop(
        "CREATE TABLE cwalls (
            id INT NOT NULL AUTO_INCREMENT,
            name TEXT,
            geo GEOMETRY NOT NULL,
            PRIMARY KEY (id)
        )",
    )?;

    // Prepare the insertion statements.
    let cwalls_stmt =
        conn.prep("INSERT INTO cwalls (name, geo) VALUES (:name, ST_GeomFromText(:geo))")?;

    // Instantiate the PBF reader.
    let file = File::open(&Path::new("germany-latest.osm.pbf")).unwrap();
    let mut reader = OsmPbfReader::new(file);

    // First pass: Collect all city walls and their node IDs.
    println!("Searching for city walls ...");

    let mut cwalls = Vec::new();
    let mut cwall_nodes = HashMap::<_, Option<(f64, f64)>>::new();

    for obj in reader.par_iter().map(Result::unwrap) {
        if let OsmObj::Way(way) = obj {
            if way.tags.contains("barrier", "city_wall") {
                cwall_nodes.extend(way.nodes.iter().map(|&id| (id, None)));
                cwalls.push(way);
            }
        }
    }

    println!(
        "Found {} city walls in total, referencing {} nodes.",
        cwalls.len(),
        cwall_nodes.len()
    );

    reader.rewind()?;

    // Second pass: Fill in node coordinates.
    println!("Searching for city wall nodes ...");

    let mut node_count = 0;

    for obj in reader.par_iter().map(Result::unwrap) {
        if let OsmObj::Node(node) = obj {
            if let Entry::Occupied(mut entry) = cwall_nodes.entry(node.id) {
                let old_id = entry.insert(Some((node.lon(), node.lat())));
                assert!(old_id.is_none(), "Duplicate node ID");

                node_count += 1;
            }
        }
    }

    println!("Found {node_count} city wall nodes in total.");

    // Walk the city walls, build line strings and insert them.
    let mut tx = conn.start_transaction(TxOpts::default())?;

    for cwall in cwalls {
        // Retrieve the name of the city wall (might be null).
        let opt_name = cwall.tags.get("name").map(|s| s.as_str());

        // Build a linestring from the nodes.
        let opt_geometry = cwall
            .nodes
            .iter()
            .map(|node_id| cwall_nodes[node_id].map(|(lon, lat)| format!("{lon:.7} {lat:.7}")))
            .collect::<Option<Vec<_>>>()
            .map(|node_strs| format!("LineString({})", node_strs.join(",")));

        // Skip those city walls without a valid geometry.
        if let Some(geometry) = opt_geometry {
            tx.exec_drop(
                &cwalls_stmt,
                params! {
                    "name" => opt_name,
                    "geo" => geometry
                },
            )?;
        }
    }

    tx.commit()?;

    Ok(())
}
