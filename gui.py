import kivy
kivy.require('1.9.1')

from kivy.app import App
from kivy.uix.label import Label
from kivy.properties import ObjectProperty
from kivy.uix.gridlayout import GridLayout
from kivy.uix.floatlayout import FloatLayout
from kivy.adapters.dictadapter import DictAdapter
from kivy.uix.listview import ListView, ListItemLabel, CompositeListItem

class Display(FloatLayout):
    
    query_in = ObjectProperty()
    data_box = ObjectProperty()

    def title_adapter(self, grid_layout):
        for i in range(3):
            grid_layout.add_widget(Label(font_size=25, color=[0, 1, 0, 1], text="colu"+str(i)))
            

    def table_adapter(self):
        datas_dict = {i: {0: str(i), 1: str(i+10), 2: str(i+20), 3: str(i+30)} for i in range(10)}
        args_converter = lambda row_index, rec: {
            'text': rec,
            'size_hint': [1, None],
            'height': 25,
            'cls_dicts': [
                {
                    'cls': ListItemLabel,
                    'kwargs': {
                        'text': "col_"+str(i)+"-{0}".format(rec[i]),
                        'font_size': 20
                    }
                } for i in range(4) ]
        }
       
        dict_adapter = DictAdapter(data=datas_dict,
                                   args_converter=args_converter,
                                   cls=CompositeListItem)

        return dict_adapter

    def update(self):

        # clear content avoid multiple listview
        self.data_box.clear_widgets()

        """
        # set up title
        coltitle = Label(font_size=30, size_hint=[1, 0.3], color=[0, 1, 0, 1])
        coltitle.text = self.query_in.text
        """

        # set up colume name bar with grid layout
        
        gridTitle = GridLayout(cols=5, size_hint=(1, 0.2))
        self.title_adapter(gridTitle)
        
        # set up datas
        listviewadapter = self.table_adapter()
        # Use the adapter in our ListView:
        list_view = ListView(adapter=listviewadapter)
        #self.data_box.add_widget(coltitle)
        self.data_box.add_widget(gridTitle)
        self.data_box.add_widget(list_view)


class DatabaseApp(App):

    def build(self):
        layout = Display()
        return layout

if __name__ == '__main__':
    DatabaseApp().run()
