from airflow.operators.email_operator import EmailOperator


class EmailOperator(EmailOperator):
    """
    Extend EmailOperator with modified templated fields.
    """

    template_fields = ('subject', 'html_content', 'params', 'files')
    ui_color = '#e0d7fb'

    def pre_execute(self, context):
        """
        This hook is triggered right before self.execute() is called.
        """

        # if you use a template in a params variable, then nested
        # template is rendered again.
        if self.params:
            context['ti'].render_templates()

        self.log.info("Sending email to: {0} \n"
                      "cc: {1}\n"
                      "bcc: {2}\n"
                      "subject: {3}\n"
                      "html_content: {4}\n"
                      "files: {5}\n"
                      .format(self.to, self.cc, self.bcc, self.subject,
                             self.html_content, self.files))

