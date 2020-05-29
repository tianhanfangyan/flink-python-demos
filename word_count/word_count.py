import os
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, BatchTableEnvironment

def word_count():
    # declare a table environment, set configurations.
    env = ExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_config = TableConfig()
    t_env = BatchTableEnvironment.create(env, t_config)

    # register Results table in table environment
    output_file = os.path.abspath('.') + '/out.txt'
    if os.path.exists(output_file):
        try:
            if os.path.isfile(output_file):
                os.remove(output_file)
        except OSError as e:
            print("Error", e.filename, e.strerror)
    print("Results:", output_file)

    sink_ddl = """
            create table Results(
                word VARCHAR,
                `count` BIGINT
            ) with (
            'connector.type' = 'filesystem',
            'format.type' = 'csv',
            'connector.path' = '{}'
            )
        """.format(output_file)
    t_env.sql_update(sink_ddl)

    # create the source table with a single string
    # preforms some transformations, and writes the results to table Results
    content = "Who's there? I think I hear them. Stand, ho! Who's there?"
    elements = [(word, 1) for word in content.split(" ")]
    t_env.from_elements(elements, ["word", "count"]) \
        .group_by("word") \
        .select("word, count(1) as count") \
        .insert_into("Results")

    # execute the Flink Python Table API job
    t_env.execute("word_count")


if __name__ == "__main__":
    word_count()
