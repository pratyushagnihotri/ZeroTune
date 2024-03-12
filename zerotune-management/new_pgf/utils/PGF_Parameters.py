import pickle

def __store_pgf_parameters(pgf_parameters):
    with open('pgf_parameters.pkl', 'wb') as f:
        pickle.dump(pgf_parameters, f)

def __load_pgf_parameters():
    try:
        with open('pgf_parameters.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        pass

def store_parameter(parameter_name, parameter_value):
    pgf_parameters = __load_pgf_parameters()
    pgf_parameters.update({parameter_name: parameter_value})
    __store_pgf_parameters(pgf_parameters)

def get_parameter(parameter_name):
    pgf_parameters = __load_pgf_parameters()
    return pgf_parameters.get(parameter_name)

def get_all_parameters():
    return __load_pgf_parameters()