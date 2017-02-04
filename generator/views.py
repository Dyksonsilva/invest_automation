from django.shortcuts import render, redirect
from .forms import Global_VariablesForm

def dashboard(request):
    return render(request, 'generator/dashboard.html')

def configuration(request):

    if request.method == 'POST':
        form = Global_VariablesForm(request.POST)
        if form.is_valid():
            post = form.save(commit=False)
            post.save()
            #Activity_EY(activity_id=post.id,
            #            ey_employee_id=Ey_employee.objects.get(ey_employee_user=request.user.id).id).save()
            return render(request, 'generator/configuration.html', {'form': form})
    else:
        form = Global_VariablesForm()
    return render(request, 'generator/configuration.html', {'form': form})
