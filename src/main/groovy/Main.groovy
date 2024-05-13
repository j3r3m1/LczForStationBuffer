package org.orbisgis.geoclimate.geoindicators

import org.orbisgis.data.jdbc.JdbcDataSource
import static org.orbisgis.data.H2GIS.open
import org.orbisgis.geoclimate.Geoindicators

// Directory where are saved the GeoClimate layers
String geoclimate_dir = "/home/decide/Software/GeoClimate/osm_Pont-de-Veyle"

// Name of the file and table containing all unit with corresponding LCZ
String outputTab = "LCZ_FIN"

// Modify initial input parameters (set only LCZ calculation)
Map input_params = Geoindicators.WorkflowGeoIndicators.getParameters()
input_params["indicatorUse"] = ["LCZ"]

// Open an H2GIS connection
h2GIS = open(File.createTempDir().toString() + File.separator + "myH2GIS_DB;AUTO_SERVER=TRUE")

main(geoclimate_dir, h2GIS, input_params, outputTab)

static void main(String inputDir, JdbcDataSource h2GIS, Map input_params, String outputTab) {

  // Load GeoClimate files into the Database
  String extension
  for (l in ["zone", "rail", "road", "building", "vegetation",
             "water", "impervious", "sea_land_mask", "urban_areas",
             "building_height_missing", "rsu"]){
    if (l == "building_height_missing"){
      extension = ".csv"
    }
    else{
      extension = ".fgb"
    }
    File filename = new File(inputDir + File.separator + l + extension)
    if (filename.exists()){
      h2GIS.load(filename.toString(), l)
    }
    else{
      println(filename.toString() + " do not exist")
    }
  }

  // Need to delete the potential existing id_rsu in the building table
  h2GIS """ALTER TABLE building DROP COLUMN ID_RSU"""

  // Create a table where to append all results at the end
  List queryFin = []

  // Run the calculation for each unit (buffer circle around station)
  h2GIS.eachRow("SELECT * FROM rsu") {row ->
    def rowMap = row.toRowResult()
    def id_rsu = rowMap."ID_RSU"

    // Create a table containing a single unit...
    h2GIS """DROP TABLE IF EXISTS RSU$id_rsu;
                    CREATE TABLE RSU$id_rsu
                      AS SELECT * FROM RSU
                      WHERE ID_RSU = $id_rsu"""
    Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(
            h2GIS,
            "zone",
            "building",
            "road",
            "rail",
            "vegetation",
            "water",
            "impervious",
            "building_height_missing",
            "sea_land_mask",
            "urban_areas",
            "RSU$id_rsu",
            input_params,
            "rsu$id_rsu")

    // Add the table to the list of tables to union
    queryFin.add("SELECT * FROM rsu${id_rsu}_rsu_lcz")

  }
  // Union all LCZ
  h2GIS """"DROP TABLE IF EXISTS $outputTab;
                      CREATE TABLE $outputTab
                      AS ${queryFin.join(" UNION ALL ")}"""

  // Save results
  h2GIS.save(outputTab, inputDir + File.separator + outputTab + ".fgb")
}