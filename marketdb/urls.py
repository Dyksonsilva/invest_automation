from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.dashboard_escolha, name='dashboard_escolha'),
    url(r'diario', views.dashboard_diario, name='dashboard_diario'),
    url(r'mensal', views.dashboard_mensal, name='dashboard_mensal'),
]
