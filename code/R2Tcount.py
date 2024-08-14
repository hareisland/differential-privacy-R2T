import os 
import numpy as np 
from collections import defaultdict 
import psycopg2 
import cplex 
import time

import logging 

class QueryHandler: 
    def init(self, query_str, name, query_type): 
        self.query_str = query_str 
        self.query_name = name 
        self.query_type = query_type 
        
    #load PostgreSQL
    def get_data(self): 
        conn = psycopg2.connect(user="postgres", password="Aa123456", host="localhost", port="5432", database= "tpch") 
        cursor = conn.cursor() 
        cursor.execute(self.query_str) 
        return cursor.fetchall()
    
def ReadInput():
    global input_file_path
    global connections
    global downward_sensitivity
    global aggregation_values
    global real_query_result
    entities = []
    connections = []
    aggregation_values = []
    input_file = open(input_file_path,'r')
    
    sensitivity_dic = {}
    my_dic = {}
    my_num = 0
    for line in input_file.readlines():
        elements = line.split()
        connects = []
        aggregation_value = float(elements[0])
        
        for element in elements[1:]:
            
            element = int(element)
            if element in my_dic.keys():
               element = my_dic[element]
            else:
                entities.append(my_num)
                my_dic[element] = my_num
                element = my_num
                my_num+=1
            if element not in sensitivity_dic.keys():
                sensitivity_dic[element] = aggregation_value
            else:
                sensitivity_dic[element] += aggregation_value
			
            connects.append(element)
        connections.append(connects)
        aggregation_values.append(aggregation_value)
    real_query_result = sum(aggregation_values)
    return real_query_result

def formulate_and_solve_linear_problem(query_data, threshold): 
    num_vars = len(query_data) 
    indices = defaultdict(list)
    
    #initialize
    my_obj = np.ones(num_vars)
    my_var = [f'u{i}' for i in range(1, num_vars + 1)]
    my_ub = np.ones(num_vars)

    for i, tpl in enumerate(query_data):
        indices[tpl].append(my_var[i])

    lin_expr = [[var_list, np.ones(len(var_list))] for var_list in indices.values()]

    problem = cplex.Cplex() ###
    problem.set_log_stream(None)
    problem.set_error_stream(None)
    problem.set_warning_stream(None)
    problem.set_results_stream(None)

    #CPLEX
    problem.objective.set_sense(problem.objective.sense.maximize)
    problem.variables.add(obj=my_obj, ub=my_ub, names=my_var)
    problem.linear_constraints.add(lin_expr=lin_expr, senses='L' * len(lin_expr), rhs= [threshold] * len(lin_expr))
    
    #run solve
    problem.solve()
    objective_value = problem.solution.get_objective_value()

    return objective_value

#random L noise
def generate_laplace_noise(scale): 
    return np.random.laplace(scale=scale)

#(e, B, GS_Q)
def apply_r2t(query_data, parameters): 
    beta, epsilon, GSQ = parameters["beta"], parameters["epsilon"], parameters["GSQ"]

    tilde_Q = 0
    for j in range(1, int(np.log2(GSQ)) + 1):
        tau = 2 ** j 
        Q_tau_j = formulate_and_solve_linear_problem(query_data, tau)

        tilde_Q_tau = Q_tau_j + generate_laplace_noise(np.log2(GSQ) * tau / epsilon) - np.log2(GSQ) * np.log(np.log2(GSQ) / beta) * (tau / epsilon)

        tilde_Q = max(tilde_Q_tau, tilde_Q)

    query_real_result = len(query_data)
    relative_error = np.abs(query_real_result - tilde_Q) / query_real_result
    #logging.info(f"Real query result: {query_real_result}. After R2T query result: {tilde_Q}. Relative error: {relative_error * 100:.4f}%.")
    print(f"error = {tilde_Q}. Relative error: {relative_error * 100:.4f}%.")
    return relative_error

#print
'''def configure_logging(log_file): 
    logger = logging.getLogger() 
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
'''

def execute_query(query, parameters, log_file, num_runs = 5): 
    #configure_logging(log_file) 
    #logging.info(f"Running query name: {query.query_name}") 
    #logging.info(f"Running query: {query.query_str}")

    query_data = query.get_data()

    running_times = []
    relative_errors = []

    # record rumtime
    for _ in range(num_runs):
        start_time = time.time()
        relative_error = apply_r2t(query_data, parameters)
        end_time = time.time()
        running_time = end_time - start_time
        relative_errors.append(relative_error)
        running_times.append(running_time)

    data = list(zip(running_times, relative_errors))
    data.sort(key=lambda x: x[1])
    data = data[20:-20]
    running_times, relative_errors = zip(*data)
    print(f"Relative error is {relative_errors}")

    avg_running_time = sum(running_times) / len(running_times)
    avg_relative_error = sum(relative_errors) / len(relative_errors)

    logging.info(f"Average running time: {avg_running_time:.4f} seconds")
    logging.info(f"Average relative error is: {avg_relative_error * 100:.4f}%")

    return avg_running_time, avg_relative_error

def main(): 
    parameters = { "epsilon": 0.8, "beta": 0.1, "GSQ": 10**5 }

    queries = [query2, query3_1, query3_2]

    for query in queries:
        log_file = f'Result/{query.query_name.replace(" ", "_")}.txt'
        print(f"Running query name: {query.query_name}")

        avg_running_time, avg_relative_error = execute_query(query, parameters, log_file)

        print(f"Average running time: {avg_running_time:.4f} seconds")
        print(f"Average relative error is: {avg_relative_error * 100:.4f}%")
        
        
        
if __name__ == "__main__":
    main()       
        
#all queries BASE + COUNT + SUM

query1 = QueryHandler('''SELECT 1, e1.from_id, e2.from_id, e2.to_id FROM edge e1 JOIN edge e2 ON e1.to_id = e2.from_id JOIN edge e3 ON e2.to_id = e3.to_id WHERE e1.from_id = e3.from_id;''', "Query 1", "SJP")

query2 = QueryHandler('''SELECT count(*), e1.from_id, e2.from_id, e2.to_id FROM edge e1 JOIN edge e2 ON e1.to_id = e2.from_id JOIN edge e3 ON e2.to_id = e3.to_id WHERE e1.from_id = e3.from_id GROUP BY e1.from_id, e2.from_id, e2.to_id;''', "Query 2", "SPJA")

query3_1 = QueryHandler('''SELECT COUNT(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey;''', "Query 3-1", "Primary Private Relation (single join key)")

query3_2 = QueryHandler('''SELECT COUNT(*) FROM supplier, lineitem, orders, customer, nation, region WHERE supplier.S_SUPPKEY = lineitem.L_SUPPKEY AND orders.O_CUSTKEY = customer.C_CUSTKEY AND customer.C_NATIONKEY = nation.N_NATIONKEY AND nation.N_NATIONKEY = supplier.S_NATIONKEY AND region.R_REGIONKEY = nation.N_REGIONKEY;''', "Query 3-2", "Primary Private Relation (multiple join key)")

query4 = QueryHandler('''SELECT SUM(l_quantity) FROM customer, orders, lineitem WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey;''', "Query 4", "Summation")