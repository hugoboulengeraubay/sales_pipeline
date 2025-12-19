import logging

def setup_logging(level=logging.INFO):
    """
    Configure le logging pour tout le projet.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
