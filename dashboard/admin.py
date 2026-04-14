from django.contrib import admin

from dashboard.models import Appointment, CxProject, DashboardDataScope, Door, Project, SyncRun

admin.site.register(Project)
admin.site.register(CxProject)
admin.site.register(Door)
admin.site.register(Appointment)
admin.site.register(SyncRun)
admin.site.register(DashboardDataScope)
