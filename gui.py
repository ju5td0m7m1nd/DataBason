import os
import re
import kivy
kivy.require('1.9.1')
from database import Database
from table_schema import Table
from kivy.app import App
from kivy.uix.popup import Popup
from kivy.uix.label import Label
from kivy.core.audio import SoundLoader
from kivy.properties import ObjectProperty
from kivy.properties import BooleanProperty
from kivy.uix.gridlayout import GridLayout
from kivy.uix.floatlayout import FloatLayout
from kivy.adapters.dictadapter import DictAdapter
from kivy.uix.listview import ListView, ListItemLabel, CompositeListItem

class LoadDialog(FloatLayout):
    load = ObjectProperty(None)
    cancel = ObjectProperty(None)

class Display(FloatLayout):
    
    loadfile = ObjectProperty()
    
    query_in = ObjectProperty()
    data_box = ObjectProperty()
    sound = ObjectProperty()
    mute_flag = not BooleanProperty()
    
    # dismiss pop up of the load file area
    def dismiss_popup(self):
        self._popup.dismiss()

    # pop up the load file area
    def show_load(self):
        content = LoadDialog(load=self.load, cancel=self.dismiss_popup)
        self._popup = Popup(title="Load file", content=content, size_hint=(0.9, 0.9))
        self._popup.open()
    
    # pop up load file
    def load(self, path, filename):
        with open(os.path.join(path, filename[0])) as stream:
            self.query_in.text = stream.read()

        self.dismiss_popup()
    
    def mute(self):
        self.mute_flag = not self.mute_flag

    def sound_control(self, which):
        if which == 'create':
            self.sound = SoundLoader.load("table_created.mp3")
        elif which == 'insert':
            self.sound = SoundLoader.load("data_inserted.mp3")
        elif which == 'select':
            self.sound = SoundLoader.load("data_selected.mp3")
        else:
            self.sound = SoundLoader.load("runtimeerror.mp3")

        if self.sound.status != 'stop':
            self.sound.stop()
        
        if not self.mute_flag:
            self.sound.play()

    def update(self):

        # clear content avoid multiple listview.
        self.data_box.clear_widgets()

        # parse query list.
        query_list = re.split(';', self.query_in.text.strip())
        if '' in query_list:
            query_list.remove('')
        # prepare the whole table title
        table_title = Label(font_size=30, size_hint=[1, 0.2], color=[0, 1, 0, 1])
        
        for query in query_list:
            try:
                self.table = db.processQuery(query)
                if db.command == 'create':
               #     self.sound_control('create')
                    table_title.text = self.table.tableName
                if db.command == 'insert':
               #     self.sound_control('insert')
                    table_title.text = self.table.tableName
                if db.command == 'select':
                #    self.sound_control('select')
                    table_title.text = "SELECT result"
                self.error = False
            except RuntimeError as e:
                self.error = True
                table_title.text = str(e)
               # self.sound_control('error')

        self.data_box.add_widget(table_title)
        if not self.error:
            # set up colume name bar with grid layout
            gridCol_Names = GridLayout(cols=5, size_hint=(1, 0.2))
            self.title_adapter(gridCol_Names, db.command)
            self.data_box.add_widget(gridCol_Names)

            # set up datas
            listviewadapter = self.table_adapter(db.command)
            # Use the adapter in our ListView:
            list_view = ListView(adapter=listviewadapter)
            self.data_box.add_widget(list_view)
            

    def title_adapter(self, grid_layout, dbCommand):

        table = self.table
        self.titles = []

        if dbCommand == 'select':
            for k in table.keys():
                self.titles.append(k)
                grid_layout.add_widget(Label(font_size=25, color=[0, 1, 0, 1], text=k))
        else:
            for attr in sorted(table.attributeList):
                self.titles.append(attr)
                if attr == table.primaryKey and dbCommand != 'select':
                    grid_layout.add_widget(Label(font_size=25, color=[1, 0, 0, 1], text=attr))
                else:
                    grid_layout.add_widget(Label(font_size=25, color=[0, 1, 0, 1], text=attr))

    def table_adapter(self, dbCommand):

        table = self.table
        titles = self.titles
        if len(titles) == 0:
            datas_dict = {0: {0: 'Result Empty'}}
            listviewRange = 1

        elif dbCommand == 'select':
            datas_dict = {i: {j:
                    table[titles[j]][i]
                    for j in range(len(titles))}
                for i in range(len(table[titles[0]]))}

            listviewRange = len(titles)
        else:
            datas_dict = {i: {j:
                    table.records.values()[i][titles[j]]
                    for j in range(len(table.attributeList))}
                for i in range(len(table.records.keys()))}

            listviewRange = len(table.attributeList)

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
                } for i in range(listviewRange) ]
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
