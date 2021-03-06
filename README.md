# Cosmian Dataiku Plugin

## DSS install

Follow instructions at :

https://www.dataiku.com/product/get-started/linux/


## DSS start

`DATA_DIR` is the data directory chosen during install 

    ${DATA_DIR}/bin/dss start


## Add the Plugin to DSS

Go to http://localhost:11000/plugins-explore/store/ 

-> Add Plugin Button

-> Fetch from Get repository



## DSS plugin development

First register to the [Dataiku Academy](https://academy.dataiku.com/plugin-development/514705)
and follow environment install instructions on that page

Introduction also available in this [documentation](https://doc.dataiku.com/dss/latest/plugins/reference/index.html?highlight=plugin%20development)

Select the created dataiku_dev_env environment when developing.

Dataiku:

 - [parameters types](https://doc.dataiku.com/dss/latest/plugins/reference/params.html)

## Cosmian developers

For simplicity the cosmian_lib must be packaged inside the plugin.
An easy way to guarantee synchronization is to:

- develop changes inside the cosmian_server/python/cosmian_lib directory (i.e. the reference source)
- then synchronize the changes into the python-lib/ directory by running from the root of thi project:

        rsync -a ../cosmian_server/python/cosmian_lib python-lib --exclude=__pycache__ 

This should be run once at first; the .env directory will help vscode pickup the library



## OLD Stuff
Font awsome 3.2.1 list: https://fontawesome.com/v3.2.1/icons/