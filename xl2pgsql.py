import xlrd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select, insert, update

# Ownership information
__author__ = 'Md. Imam Hossain Roni'
__copyright__ = "Copyright 2020, Roni"
__credits__ = ["Roni"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "Roni"
__email__ = "imamhossainroni95@gmail.com"
__status__ = "Development"


class Xl2Db:
    def __init__(self, engine, metadata, file_src):
        self.engine = engine
        self.metadata = metadata
        self.file_src = file_src

    @property
    def raw_data_from_xl(self):
        xl_file = self.file_src
        _book = xlrd.open_workbook(xl_file)
        sheet_data = _book.sheet_by_name("source")
        return sheet_data

    def get_tables(self):
        tbl_obj = self.metadata.tables.items()
        return tbl_obj

    def process(self):
        for r in range(1, self.raw_data_from_xl.nrows):
            name = self.raw_data_from_xl.cell(r, 0).value
            email = self.raw_data_from_xl.cell(r, 1).value
            contact_no = self.raw_data_from_xl.cell(r, 2).value
            status = self.raw_data_from_xl.cell(r, 3).value
            values = (name, email, contact_no, status)
            for name, table in self.get_tables():
                try:
                    query = insert(table, values)
                    self.engine.execute(query)
                except Exception as e:
                    print(e)

    def show_data(self):
        query = select([table for name, table in self.get_tables()])
        row_data = self.engine.execute(query)
        result = [list(each) for each in row_data]
        print(result)


def main():
    engine = create_engine('postgresql://postgres:test@localhost/exam_test', echo=False)
    metadata = MetaData()
    metadata.bind = engine
    metadata.reflect(engine)
    file_src = "data/friend_list.xlsx"
    xl_to_db = Xl2Db(engine, metadata, file_src)
    xl_to_db.process()
    xl_to_db.show_data()


if __name__ == '__main__':
    main()
