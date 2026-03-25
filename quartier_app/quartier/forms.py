from django import forms


class SearchForm(forms.Form):
    q = forms.CharField(required=False, label='Recherche')
    rayon = forms.IntegerField(required=False, min_value=1, initial=300)
