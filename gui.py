import kivy
kivy.require('1.9.1')
from kivy.app import App
from kivy.adapters.dictadapter import DictAdapter
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.gridlayout import GridLayout
from kivy.properties import ObjectProperty, StringProperty
from kivy.uix.listview import ListView, ListItemLabel, CompositeListItem

class Display(FloatLayout):
    
    coltitle = ObjectProperty()
    query_in = ObjectProperty()
    data_box = ObjectProperty()

    def enter(self):
        self.colname.text = self.query_in.text

    def update(self):
        integers_dict = {str(i): {'text': str(i), 'id': str(i+100)} for i in range(50)}
        args_converter = lambda row_index, rec: {
            'text': rec,
            'size_hint_y': None,
            'height': 25,
            'cls_dicts': [{
                                'cls': ListItemLabel,
                                'kwargs': {
                                    'text': "col_0-{0}".format(rec['text']),
                                    'font_size': 20}},
                            {
                                'cls': ListItemLabel,
                                'kwargs': {
                                    'text': "col_1-{0}".format(rec['id']),
                                    'font_size': 20}},
                            {
                                'cls': ListItemLabel,
                                'kwargs': {
                                    'text': "col_2-{0}".format(rec['text']),
                                    'font_size': 20}},
                            {
                                'cls': ListItemLabel,
                                'kwargs': {
                                    'text': "col_3-{0}".format(rec['id']),
                                    'font_size': 20}},
                            {
                                'cls': ListItemLabel,
                                'kwargs': {
                                    'text': "col_4-{0}".format(rec['text']),
                                    'font_size': 20}}
                            ]}

        item_strings = ["{0}".format(index) for index in range(50)]

        dict_adapter = DictAdapter(sorted_keys=item_strings,
                                   data=integers_dict,
                                   args_converter=args_converter,
                                   cls=CompositeListItem)

        # Use the adapter in our ListView:
        list_view = ListView(adapter=dict_adapter)
        self.data_box.add_widget(list_view)


class DatabaseApp(App):

    def build(self):
        layout = Display()
        return layout

if __name__ == '__main__':
    DatabaseApp().run()
