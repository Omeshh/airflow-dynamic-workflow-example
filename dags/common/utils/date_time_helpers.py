import pendulum


def get_est_datetime():
    """Returns current EST time in datetime format"""
    return pendulum.now('UTC').in_timezone('EST').strftime("%Y-%m-%d %H:%M:%S")
