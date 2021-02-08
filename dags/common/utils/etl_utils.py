def apply_transformations(df, transformations):
    """
    Apply transformations to pandas data frame
    :param df: Pandas data frame on which to apply transformations
    :type df: Pandas data frame
    :param transformations: A dictionary of transformations.
        Example: transformations={
            "RENAME:Billingcycle": "BillingCycle",
            "ID": 1234,
            "AccountNumber": lambda row: row['AccountNumber'].strip(),
            "CustomerName": lambda row: "{} {}".format(row["FirstName"].strip(), row["LastName"].strip()[:100]),
            "Address": lambda row: ctds.SqlVarChar(row["Address"].encode("utf-16le")),
            "FILTER:CheckEmpty": lambda row: row['AccountNumber'].strip() != ""}
    :type transformations: dict
    :return: A generator object that returns rows as a dictionary
    """
    for column, transformation in transformations.items():
        if ':' in column:
            (operation, col) = column.split(':')
            if operation == 'FILTER':
                df = df[df.apply(transformation, axis=1)]
            elif operation == 'RENAME':
                df.rename(columns={col: transformation}, inplace=True)
        elif callable(transformation):
            df[column] = df.apply(transformation, axis=1, result_type='reduce')
        else:
            df[column] = transformation

    for record in df.to_dict('records'):
        yield record
