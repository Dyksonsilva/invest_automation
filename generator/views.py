from django.shortcuts import render
from .models import Global_Variables

def dashboard(request):
    return render(request, 'generator/dashboard.html')

def configuration(request):

    control_status = 0

    if request.method == 'POST':
        variable_values = list(Global_Variables.objects.values_list("variable_value", flat=True))
        variable_update = request.POST.getlist('variable_update')

        for i in range(0, len(variable_values)):
            if variable_values[i] != variable_update[i]:
                print('variable_values: ' + variable_values[i])
                print('variable_update: ' + variable_update[i])
                global_object = Global_Variables.objects.get(id=(i+1))
                global_object.variable_value = variable_update[i]
                global_object.save()
                control_status = 1
            elif control_status == 0:
                control_status = 2

    return render(request, 'generator/configuration.html', {'Global_Variables': Global_Variables.objects.all(), 'control_status' : control_status})
