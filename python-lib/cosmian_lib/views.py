from .context import Context


class Views():

    def __init__(self, context: Context):
        self.context = context

    def list(self) -> tuple:
        """
        List all the views on the server
        """
        return self.context.get("/views", None, "Views::failed listing the views")

    def delete(self, view_name):
        """
        Delete the given view
        """
        return self.context.delete("/view/%s" % view_name,
                                   error_message="Views::Error deleting view: %s" % view_name,
                                   allow_404=True)

    def retrieve(self, view_name):
        """
        Retrieve a view and return it or None if not found
        """
        return self.context.get("/view/%s" % view_name, None,
                                "Views::failed retrieving the view: %s" % view_name, allow_404=True
                                )

    def update(self, view: dict):
        """
        Update a view. Will fail if the view does not exist
        """
        self.context.put("/view", view,
                         error_message="Views::failed updating the view"
                         )
        return {'status': 'ok', 'msg': 'Updated the view'}

    def create(self, view):
        """
        Create a view. Will fail if the view already exists
        """
        self.context.post("/view", view,
                          error_message="Views::failed creating the view"
                          )
        return {'status': 'ok', 'msg': 'Created the view'}
