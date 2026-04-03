import pandas as pd
from datetime import datetime
import os

SESSION_FILE = "data/session/session_interactions.csv"


def log_interaction(user_id, product_id, action):
    """
    Ghi lại hành vi người dùng trong session

    Parameters
    ----------
    user_id : int
    product_id : int
    action : str
        view / like / add_to_cart / purchase
    """

    row = {
        "user_id": user_id,
        "product_id": product_id,
        "action": action,
        "timestamp": datetime.now()
    }

    df = pd.DataFrame([row])

    os.makedirs("data/session", exist_ok=True)

    if not os.path.exists(SESSION_FILE):
        df.to_csv(SESSION_FILE, index=False)
    else:
        df.to_csv(SESSION_FILE, mode="a", header=False, index=False)