import time, json, logging, os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from libraries.drivers.talkwalker.talkwalkerdriver import TalkWalkerDriver

if __name__ == "__main__":
    # initialize all sub systems
    # logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    env_var_name = 'TALKWALKER_ARGS'

    logger.info("=== Talkwalker driver initialized. ===")

    if env_var_name not in os.environ or not os.environ[env_var_name]:
        logger.error(f"The environment variable {env_var_name} is missing or has a null value.")
        sys.exit(1)

    logger.info(f"arguments for this run in env var  = {os.environ[env_var_name]}")

    args = os.environ[env_var_name]

    args_dict = json.loads(args)

    # convert news links to a bool

    args_dict["get_news_links"] = (args_dict["get_news_links"].casefold() == "true".casefold())

    logger.info(f'parsed arguments dict is {args_dict}')

    driver = TalkWalkerDriver()
    driver.run(args_dict)
    driver = None

    logger.info("done")
    logger.info("=== Talkwalker driver completed. ===")

    exit(0)


