from prefect import flow

# TODO: https://docs.prefect.io/integrations/prefect-github

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Btibert3/882-f25-class-project.git",
        entrypoint="prefect/flows/hello-world.py:simple_flow",
    ).deploy(
        name="882-fall25",
        work_pool_name="brock-worker1",
        job_variables={"env": {"BROCK": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        tags=["prod"],
        description="Basic Hello World",
        version="1.0.0",
    )