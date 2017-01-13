from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect

def dashboard(request):

    return render(request, 'generator/dashboard.html')
