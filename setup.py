import sys
from src.scripts.setup_codebuild import codebuild_init
from src.scripts.setup_lambda import lambda_init
from src.scripts.setup_s3 import s3_init
from utils.configs import config

setup_pipeline = [
    ("S3", s3_init),
    ("Lambda", lambda_init),
    ("CodeBuild", codebuild_init),
]


def validate_configs() -> None:
    """Raise ValueError for any config key that has no value set."""
    print("Checking configs...")
    missing = [key for key, value in config.items() if value is None]
    if missing:
        raise ValueError(f"Missing values for config keys: {missing}. Set them in your .env file.")


def setup_resources() -> None:
    """Run each init step in order. Stops and raises on the first failure."""
    for label, init_fn in setup_pipeline:
        print(f"Setting up {label}...")
        if not init_fn():
            raise RuntimeError(f"{label} initialisation failed — aborting setup.")
        print(f"{label} setup complete.")


if __name__ == "__main__":
    try:
        validate_configs()
        setup_resources()
    except (ValueError, RuntimeError) as exc:
        print(str(exc))
        sys.exit(1)

    print("All resources set up successfully.")

