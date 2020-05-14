# Cosmian Dataiku Plugin



## Developing

For simplicity the cosmian_lib must be packaged inside the plugin.
An easy way to guarantee synchronization is to:
    - develop changes inside the cosmian_server/python/cosmian_lib directory (i.e. the reference source)
    - then synchronize the changes into the python-lib/ directory by running:

        rsync -a ../cosmian_server/python/cosmian_lib python-lib --exclude=__pycache__ 

This should be run once at first; the .env directory will help vscode pickup the library



## OLD Stuff
Font awsome 3.2.1 list: https://fontawesome.com/v3.2.1/icons/