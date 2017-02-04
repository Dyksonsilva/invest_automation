from django.forms import ModelForm
from .models import Global_Variables

class Global_VariablesForm(ModelForm):

    class Meta:
        model = Global_Variables
        fields = (
            'id', 'variable_name', 'variable_value')
