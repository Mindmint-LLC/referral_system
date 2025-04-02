
from prefect import flow
from dbt_directory.dbt_run import run_dbt_flow
from post_credits import post_credits


@flow
def mm_referral_program():
    run_dbt_flow()
    post_credits()


if __name__ == "__main__":
    mm_referral_program.serve()