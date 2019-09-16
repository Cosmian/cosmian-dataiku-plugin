Dataiku - DSS in Docker Ubuntu 18.04
====================================

This provides a running instance of Dataiku DSS in a Docker
Usefull for unsupported linuxes suchas Ubuntu 19

Installation
------------

  1. Build the docker using `./build.sh`
  2. Install the permanent files using `install.sh`. 
  By default this goes into `~/dss_home` but can be changed in the script
  by updating the `DSS_HOME` variable
  
Running
-------
  
Run the docker `run.sh`. Do not forget to update the `DSS_HOME` variable
  in that script if you changed it in the `install.sh` script
  
DSS is available at http://localhost:11000

To stop the DSS, type `sudo docker stop dataiku_dss`, it takes a while...

The port can be adjusted in the `run.sh` script     
