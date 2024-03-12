import sys
import argparse
from learning.constants import Cost_Optimizer
from ortools.linear_solver import pywraplp

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, required=True)
    parser.add_argument('--mode', required=True,
                       choices=[Cost_Optimizer.PREDICTED_COSTS, Cost_Optimizer.REAL_COSTS])
    args = parser.parse_args()

    # search for best parallelism degree by looking at the costs
    solver = pywraplp.Solver.CreateSolver('GLOP')   # alternative: PDLP
    if not solver:
        print('Could not create solver')
        sys.exit(1)
    #generate cost variables to be used in  objective function
    latency = solver.NumVar(0, solver.infinity(), 'latency')
    tp = solver.NumVar(0, solver.infinity(), 'throughput')
    # generate variable for every operator regarding parallelism degree in a for loop
    # test
    p1 = solver.NumVar(0, solver.infinity(), 'p1')
    p2 = solver.NumVar(0, solver.infinity(), 'p2')
    p3 = solver.NumVar(0, solver.infinity(), 'p3')

    
    # add constraints
    #test
    solver.Add(3*p1+3*p2+3*p3 == 100)
    solver.Add(5*p1+5*p2+2*p3 == 100)
    solver.Add(8*p1+8*p2+8*p3 == 400)


    # add objective function
    solver.Maximize(tp-latency)

    # invoke solver
    status = solver.Solve()
    if status == pywraplp.Solver.OPTIMAL:
        print('Solution:')
        print('Objective value =', solver.Objective().Value())
        print('latency =', latency.solution_value())
        print('throughput =', tp.solution_value())
    else:
        print('The problem does not have an optimal solution.')
