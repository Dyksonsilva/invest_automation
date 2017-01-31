from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.dashboard_escolha, name='dashboard_escolha'),
]
