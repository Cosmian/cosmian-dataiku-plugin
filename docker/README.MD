Dataiku - DSS in Docker Ubuntu 18.04
====================================

This docker provides a running instance of Dataiku DSS.
Useful to run DSS on unsupported linuxes such as Ubuntu 19

Installation
------------

  1. Build the docker using `sudo ./build.sh`
  2. Install the permanent local files using `sudo ./install.sh`. 
  By default these go into `~/dss_home` but can be changed in the script
  by updating the `DSS_HOME` variable
  
Running
-------
  
Run the docker `sudo ./run.sh`. Do not forget to update the `DSS_HOME` variable
  in that script if you changed it in the `install.sh` script
  
DSS is available at http://localhost:11000

To stop DSS, type `sudo docker stop dataiku_dss`, it takes a while...

The port can be adjusted in the `run.sh` script     
