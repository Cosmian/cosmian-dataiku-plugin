import os, sys, json, logging

def _is_running_remotely():
    if os.path.isfile("remote-run-env-def.json"):
        with open("remote-run-env-def.json", 'r') as fd:
            return json.load(fd).get("runsRemotely", False)
    else:
        return False

def _get_remote_run_env_def(no_fail=True):
    if os.path.isfile("remote-run-env-def.json"):
        with open("remote-run-env-def.json", 'r') as fd:
            return json.load(fd)
    elif no_fail:
        return {'env':os.environ, 'python':{}, 'r':{}}
    else:
        raise Exception("No file remote-run-env-def.json found in cwd")
        
DKU_REMOTE_RUN_ENV_DEF = None
def get_remote_run_env_def(no_fail=True):
    global DKU_REMOTE_RUN_ENV_DEF
    if DKU_REMOTE_RUN_ENV_DEF is None:
        DKU_REMOTE_RUN_ENV_DEF = _get_remote_run_env_def(no_fail)
    return DKU_REMOTE_RUN_ENV_DEF
    
def _get_dku_env_vars(no_fail=True):
    return get_remote_run_env_def(no_fail).get('env', {})

def get_env_vars(no_fail=True):
    dku_vars = _get_dku_env_vars(no_fail)
    sys_vars = os.environ
    vars = {}
    vars.update(sys_vars)
    vars.update(dku_vars) # takes precedence
    return vars

NO_DEFAULT=object()
#def get_dku_env_var(k, d=NO_DEFAULT, no_fail=True):
#    env_vars = _get_dku_env_vars(no_fail)
#    if d == NO_DEFAULT:
#        return env_vars.get(k)
#    else:
#        return env_vars.get(k, d)

#def has_dku_env_var(k, no_fail=True):
#    return k in _get_dku_env_vars(no_fail)

def get_env_var(k, d=NO_DEFAULT, no_fail=True):
    dku_vars = _get_dku_env_vars(no_fail)
    if k in dku_vars:
        return dku_vars.get(k)
    sys_vars = os.environ
    if d == NO_DEFAULT:
        return sys_vars.get(k)
    else:
        return sys_vars.get(k, d)
        
def has_env_var(k, no_fail=True):
    return k in _get_dku_env_vars(no_fail) or k in os.environ

def get_dkuflow_spec(no_fail=True):
    if 'flowSpec' in get_remote_run_env_def(no_fail):
        return get_remote_run_env_def(no_fail).get("flowSpec")
    spec_str = get_env_var('DKUFLOW_SPEC')
    if spec_str is not None and len(spec_str) > 0:
        return json.loads(spec_str)
    else:
        return None
        
def set_dku_env_var_and_sys_env_var(k, v, no_fail=True):
    dku_vars = _get_dku_env_vars(no_fail)
    dku_vars[k] = v
    sys_vars = os.environ
    sys_vars[k] = v
    
    
# because putting unicode in os.environ fails (python2, obviously)
def safe_os_environ_update(added):
    if sys.version_info > (3,0):
        os.environ.update(added)
    else:
        for k in added:
            v = added[k]
            # do the value
            if v is not None and isinstance(v, unicode):
                v = v.encode('utf8')
            # do the key
            if k is not None and isinstance(k, unicode):
                k = k.encode('utf8')
            os.environ[k] = v

def read_dku_env_and_set(no_fail=True, force=False):
    global DKU_REMOTE_RUN_ENV_DEF
    if force:
        DKU_REMOTE_RUN_ENV_DEF = None
    from dataiku.core import flow # imported here to avoid cyclic dep
    DKU_REMOTE_RUN_ENV_DEF = get_remote_run_env_def(no_fail)
    safe_os_environ_update(DKU_REMOTE_RUN_ENV_DEF.get('env', {})) # a bit gruik, for code that still relies on os.environ
    for p in DKU_REMOTE_RUN_ENV_DEF.get('python', {}).get('pythonPathChunks', []):
        sys.path.append(p)
    flow.load_flow_spec() 
