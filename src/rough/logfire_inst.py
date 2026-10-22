import logfire
from logging import basicConfig, getLogger
import time
import gc



gc.collect()


logfire.configure()


vals = [1, 2, 3, 4, 5]

val = [x for x in range(1, 30)]  # Example list of values to test
vals = val
len(vals)
@logfire.instrument("test_func x={x}")
def test_func(x):
    y = 3*x**2 + 2*x + 1
    with logfire.span("test_func_inner",vals = x ,results =y ):
       
        logfire.info(f"Result: {y}")
    return y

@logfire.instrument("linear function call test")
def run_test():
    return [test_func(x) for x in vals]

y = run_test()




