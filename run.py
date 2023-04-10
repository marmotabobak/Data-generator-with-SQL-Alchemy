from datetime import datetime, timedelta

from app import DataGenerator, Autoclass, AutoclassDialog, AutoclassPhrase, AutoclassTheme

tables_dict = {
    'Autoclass': Autoclass,
    'Dialogues': AutoclassDialog,
    'Phrases': AutoclassPhrase,
    'Themes': AutoclassTheme
}

data_generator = DataGenerator(db_name='postgres', psswd='password', host='localhost', port=5434)
#data_generator.drop_and_create_all_tables()
data_generator.create_all_tables()
data_generator.print_current_tables_size(tables_dict=tables_dict)

rows_created_per_session = 0
print('\nAdding new data...')

calls_to_create_per_cycle = 100
phrases_to_create_per_dialog = 50
themes_to_create_per_dialog = 10

current_time = 0
time_delta = ''

while rows_created_per_session < 10_000:

    data_generator.create_data(
        calls_to_create=calls_to_create_per_cycle,
        phrases_per_dialog=phrases_to_create_per_dialog,
        themes_per_dialog=themes_to_create_per_dialog
    )

    rows_created_per_session += calls_to_create_per_cycle

    if current_time:
        time_delta = datetime.now() - current_time

    current_time = datetime.now()

    print(f'{datetime.now().strftime("%H:%M:%S")} '
          f'{f"({str(time_delta.seconds)}.{str(time_delta.microseconds // 10_000)} sec.) " if time_delta else ""}'
          f'Autoclass:{rows_created_per_session} '
          f'Dialog:{rows_created_per_session} '
          f'Phrase:{rows_created_per_session * phrases_to_create_per_dialog} '
          f'Theme:{rows_created_per_session * themes_to_create_per_dialog}')

data_generator.print_current_tables_size(tables_dict=tables_dict)
data_generator.close_session()
