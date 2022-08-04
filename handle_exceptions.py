def handle_exception(ex):
    print("-----------------------------------------------------------------------------------------------------------")
    print("Context statement: ", ex.context.statement)
    print("Error code: ", ex.context.first_error_code)
    print("Error message: ", ex.context.first_error_message)
    print("Context id: ", ex.context.client_context_id)
    print("-----------------------------------------------------------------------------------------------------------")
    print("\n")
