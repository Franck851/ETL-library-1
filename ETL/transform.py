class Transform:

  def __init__(self, transformation_function, *args, **kwargs):
    self.transformation_function = transformation_function
    self.args = args
    self.kwargs = kwargs

  def __call__(self, df):
    return self.transformation_function(df, *self.args, **self.kwargs)
