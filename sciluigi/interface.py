import luigi

def run(*args, **kwargs):
    luigi.run(*args, **kwargs) 

def run_locally():
    luigi.run(local_scheduler=True)
