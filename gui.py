import re
import kivy
kivy.require('1.9.1')
from database import Database
from table_schema import Table
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

    def update(self):

        # clear content avoid multiple listview.
        self.data_box.clear_widgets()

        # parse query list.
        query_list = re.split(';', self.query_in.text.strip())
        # prepare the whole table title
        table_title = Label(font_size=30, size_hint=[1, 0.2], color=[0, 1, 0, 1])
        
        for query in query_list:
            try:
                self.table = db.processQuery(query)
                #if db.command == 'create':
                #if db.command == 'insert':
                table_title.text = self.table.tableName
                self.error = False
            except RuntimeError as e:
                self.error = True
                table_title.text = str(e)

        self.data_box.add_widget(table_title)
        if not self.error:
            # set up colume name bar with grid layout
            gridCol_Names = GridLayout(cols=5, size_hint=(1, 0.2))
            self.title_adapter(gridCol_Names)
            self.data_box.add_widget(gridCol_Names)    
            # set up datas
            listviewadapter = self.table_adapter()
            # Use the adapter in our ListView:
            list_view = ListView(adapter=listviewadapter)
            self.data_box.add_widget(list_view)

    def title_adapter(self, grid_layout):

        table = self.table
        self.titles = []
        
        for attr in sorted(table.attributeList):
            self.titles.append(attr)
            if attr == table.primaryKey:
                grid_layout.add_widget(Label(font_size=25, color=[1, 0, 0, 1], text=attr))
            else:
                grid_layout.add_widget(Label(font_size=25, color=[0, 1, 0, 1], text=attr))

    def table_adapter(self):

        table = self.table
        titles = self.titles

        datas_dict = {i: {j:
                table.records.values()[i][titles[j]]
                for j in range(len(table.attributeList))}
            for i in range(len(table.records.keys()))}

        """
        datas_dict = {i: {j:
                    table.records.values()[i].values()[j]
                    for j in range(len(table.attributeList))}
                for i in range(len(table.records.keys()))}
        """
        args_converter = lambda row_index, rec: {
            'text': rec,
            'size_hint': [1, None],
            'height': 25,
            'cls_dicts': [
                {
                    'cls': ListItemLabel,
                    'kwargs': {
                        'text': "{0}".format(rec[i]),
                        'font_size': 20
                    }
                } for i in range(len(table.attributeList)) ]
        }
       
        dict_adapter = DictAdapter(data=datas_dict,
                                   args_converter=args_converter,
                                   cls=CompositeListItem)

        return dict_adapter


class DatabaseApp(App):

    def build(self):
        layout = Display()
        return layout


if __name__ == '__main__':
    db = Database()
    DatabaseApp().run()
