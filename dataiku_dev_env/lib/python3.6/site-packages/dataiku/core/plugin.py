from dataiku.core import intercom
import sys, imp

def use_plugin_libs(plugin_id):
    """Add the lib/ folder of the plugin to PYTHONPATH"""
    folders = intercom.jek_or_backend_json_call("plugins/get-lib-folders", data={
        "pluginId" : plugin_id
    })
    
    python_lib = folders.get('pythonLib', '')
    if len(python_lib) > 0:
        if python_lib not in sys.path:
            sys.path.append(python_lib)
    else:
        raise Exception('No python-lib folder defined in this plugin')

def import_from_plugin(plugin_id, package_name):
    """Import a package from the lib/ folder of the plugin and returns the module"""
    folders = intercom.jek_or_backend_json_call("plugins/get-lib-folders", data={
        "pluginId" : plugin_id
    })
    
    python_lib = folders.get('pythonLib', '')
    if len(python_lib) > 0:
        fp, pathname, description = imp.find_module(package_name, [python_lib])
        try:
            return imp.load_module(package_name, fp, pathname, description)
        finally:
            if fp:
                fp.close()
    else:
        raise Exception('No python-lib folder defined in this plugin')
            