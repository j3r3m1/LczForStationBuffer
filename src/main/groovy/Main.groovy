package org.orbisgis.geoclimate.geoindicators

import org.orbisgis.data.jdbc.JdbcDataSource
import static org.orbisgis.data.H2GIS.open
import org.orbisgis.geoclimate.Geoindicators

// Path of the file containing the buffer geometries around stations
File buffer_dir = new File("/home/decide/Software/GeoClimate/osm_Pont-de-Veyle/rsu.fgb")

// Path and name of the resulting file with LCZ type for each station buffer
String outputPath = "/tmp/LCZ_FOR_STATION_BUFFER.fgb"

// Directory where are saved the GeoClimate layers
String geoclimate_dir = "/home/decide/Software/GeoClimate/osm_Pont-de-Veyle"

// Name of the file and table containing all unit with corresponding LCZ
String outputTab = "LCZ_FIN"

// Modify initial input parameters (set only LCZ calculation)
Map input_params = Geoindicators.WorkflowGeoIndicators.getParameters()
input_params["indicatorUse"] = ["LCZ"]

// Open an H2GIS connection
h2GIS = open(File.createTempDir().toString() + File.separator + "myH2GIS_DB;AUTO_SERVER=TRUE")

main(geoclimate_dir, h2GIS, input_params, outputTab, buffer_dir, outputPath)

static void main(String inputDir, JdbcDataSource h2GIS, Map input_params, String outputTab, File buffer_dir, String outputPath) {

  // Load GeoClimate files into the Database
  String extension
  for (l in ["zone", "rail", "road", "building", "vegetation",
             "water", "impervious", "sea_land_mask", "urban_areas",
             "building_height_missing"]){
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
  // Load the RSU (which are the buffer around station) into the database
  if (buffer_dir.exists()){
    h2GIS.load(buffer_dir.toString(), "rsu")
  }
  else{
    println(buffer_dir.toString() + " do not exist")
  }

  // Create a table where to append all results at the end
  List queryFin = []
  Map rsu_lcz_list = [:]

  // Run the calculation for each unit (buffer circle around station)
  h2GIS.eachRow("SELECT * FROM rsu") {row ->
    def rowMap = row.toRowResult()
    def id_rsu = rowMap."ID_RSU"

    // Need to delete the potential existing id_rsu in the building table
    h2GIS """DROP TABLE IF EXISTS building_tempo;
            CREATE TABLE building_tempo
                AS SELECT * EXCEPT(ID_RSU)
                FROM building"""

    // Create a table containing a single unit...
    h2GIS """DROP TABLE IF EXISTS RSU$id_rsu;
                    CREATE TABLE RSU$id_rsu
                      AS SELECT * FROM RSU
                      WHERE ID_RSU = $id_rsu"""
    rsu_lcz_list[id_rsu] = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(
                               h2GIS,
                              "zone",
                              "building_tempo",
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
                              "rsu$id_rsu")["rsu_lcz"]

    // Add the table to the list of tables to union
    queryFin.add("SELECT * FROM ${rsu_lcz_list[id_rsu]}")

  }
  // Union all LCZ
  h2GIS """DROP TABLE IF EXISTS $outputTab;
                      CREATE TABLE $outputTab
                      AS ${queryFin.join(" UNION ALL ")}"""

  // Save results
  h2GIS.save(outputTab, outputPath)
}