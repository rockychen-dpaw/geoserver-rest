import os
import logging
import traceback
import jinja2

from . import timezone
from . import settings
from . import utils
from . import loggingconfig
from .geoserverhealthcheck import GeoserverHealthCheck
from .mail import EmailMessage
from .tasks import OutOfSyncTask

logger = logging.getLogger("geoserver_rest.geoservershealthcheck")

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader([settings.BASE_DIR]),
    autoescape=jinja2.select_autoescape()
)

class GeoserversHealthCheck(object):
    
    def __init__(self,geoservers,requestheaders=None,dop=1):
        self.healthchecks = [GeoserverHealthCheck(geoserver[0],geoserver[1],geoserver[2],geoserver[3],requestheaders=requestheaders,dop=dop,keep_tasks=True) for geoserver in geoservers]

    def start(self):
        for healthcheck in self.healthchecks:
            healthcheck.start()

    @property
    def tasks(self):
        tasks = 0
        for healthcheck in self.healthchecks:
            tasks += healthcheck.tasks or 0

        return tasks

    @property
    def warnings(self):
        warnings = 0
        for healthcheck in self.healthchecks:
            warnings += healthcheck.warnings or 0

        return warnings
        
    @property
    def errors(self):
        errors = 0
        for healthcheck in self.healthchecks:
            errors += healthcheck.errors or 0

        return errors

    def wait_to_finish(self):
        processing_metadatas = []
        #wait to finish
        for healthcheck in self.healthchecks:
            healthcheck.wait_to_finish(close_report_writer=False)

        #check the sync status among geoservers
        tasks = self.healthchecks[0].finished_tasks
        for healthcheck in self.healthchecks[1:]:
            slavetasks = healthcheck.finished_tasks
            for key,task in tasks.items():
                if key in slavetasks:
                    continue
                healthcheck._warningwriteaction(OutOfSyncTask(task,missing=True))
                healthcheck.metadata["errors"] += 1

            for key,task in slavetasks.items():
                if key in tasks:
                    continue
                healthcheck._warningwriteaction(OutOfSyncTask(task,missing=False))
                healthcheck.metadata["warnings"] += 1

        #close report writers
        for healthcheck in self.healthchecks:
            healthcheck.close_report_writers()

        #write report writers
        for healthcheck in self.healthchecks:
            healthcheck.write_report()

        #get the metadata
        for healthcheck in self.healthchecks:
            processing_metadatas.append({"healthcheck":healthcheck,"processing_metadata":healthcheck.metadata})

        return processing_metadatas


if __name__ == '__main__':
    parse_url = lambda data: [ d.strip() for d in data.rsplit("=",1)] if "=" in data else (utils.get_domain(data),data)
    geoserver_urls = os.environ["GEOSERVER_URLS"]
    geoserver_urls = [ parse_url(u.strip()) for u in geoserver_urls.split(",") if u.strip()]
    geoserver_users = os.environ.get("GEOSERVER_USERS") or os.environ.get("GEOSERVER_USER") 
    geoserver_users = [u.strip() for u in geoserver_users.split(",") if u.strip()]
    if len(geoserver_users) == 1:
        geoserver_users = geoserver_users * len(geoserver_urls)
    elif len(geoserver_users) != len(geoserver_urls):
        raise Exception("The count({1}) of geoserver users does not match with the count({0}) of geoserver urls.".format(len(geoserver_urls),len(geoserver_users)))
    
    geoserver_passwords = os.environ.get("GEOSERVER_PASSWORDS") or os.environ.get("GEOSERVER_PASSWORD")
    geoserver_passwords = [u.strip() for u in geoserver_passwords.split(",") if u.strip()]
    if len(geoserver_passwords) == 1:
        geoserver_passwords = geoserver_passwords * len(geoserver_urls)
    elif len(geoserver_passwords) != len(geoserver_urls):
        raise Exception("The count({1}) of geoserver passwords does not match with the count({0}) of geoserver urls.".format(len(geoserver_urls),len(geoserver_passwords)))

    geoservers = [(geoserver_urls[i][0],geoserver_urls[i][1],geoserver_users[i],geoserver_passwords[i]) for i in range(len(geoserver_urls))]

    healthcheck = GeoserversHealthCheck(geoservers,settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"),settings.HEALTHCHECK_DOP)
    healthcheck.start()
    processing_metadatas = healthcheck.wait_to_finish()

    if settings.EMAIL_ENABLED and (any(metadata.get("exceptions") for metadata in processing_metadatas) or healthcheck.errors):
        #send email
        subject = "Some errors found on geoserver({})".format(",".join(gs[0] for gs in geoservers))
        context = {"healthchecks":processing_metadatas}
        #generate the email body
        body_template = jinja_env.get_template("notify_email.html")
        body = body_template.render(context)
        email = EmailMessage(subject=subject,body=body,from_email=settings.EMAIL_FROM,to=settings.EMAIL_TO,cc=settings.EMAIL_CC,bcc=settings.EMAIL_BCC)
        email.content_subtype = 'html'
        for hc in healthcheck.healthchecks:
            if hc.errors:
                email.attach_file(os.path.join(hc.report_dir, hc.warnings_file))
        email.send()

