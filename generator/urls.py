from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^dashboard', views.dashboard, name='dashboard'),
    url(r'^configuration', views.configuration, name='configuration'),
]
