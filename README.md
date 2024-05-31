# LczForStationBuffer
Use the GeoClimate software to calculate the LCZ for RSU being a buffer around stations

# Step to apply the code
## 1. Install Groovy (if not already installed) - to deal with groovy version errors, see the [GeoClimate FAQ](https://github.com/orbisgis/geoclimate/wiki/Frequently-Asked-Questions#groovy-version-issue)
## 2. Download the code as zip and unzip
## 3. Modify the variables "buffer_dir", "outputPath" and "geoclimate_dir" with the corresponding path to files on your computer and the variable "ID_station" according to the column name of the ID of your station (should be integer) 
## 4. Run the main.groovy script via a console using the following command:
groovy /path/to/your/file/main.groovy
