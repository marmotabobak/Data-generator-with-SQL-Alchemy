import enum
import random
from datetime import datetime, timedelta
from typing import Dict

from sqlalchemy.schema import CreateSchema
from sqlalchemy import create_engine
from sqlalchemy import Column, Text, DateTime, BigInteger, Boolean, Enum, ARRAY, ForeignKey, Float, INT
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import ProgrammingError


Base = declarative_base()


class TranscriptionTypeValues(enum.Enum):
    '''
    Copypast from model.py
    '''
    Offline = 'Offline'
    Online = 'Online'


class Autoclass(Base):
    """
    Copy-past from model.py (name changed to Autoclass)
    """

    __tablename__ = 'autoclass'
    __table_args__ = {'schema': 'autoclass'}

    # common
    EDUID = Column('EDUID', Text, quote=False)
    UCID = Column('UCID', Text, quote=False)

    # dialog
    CreationTime = Column('CreationTime', DateTime, quote=False)
    ClientNumber = Column('ClientNumber', Text, quote=False)
    OperatorDNIS = Column('OperatorDNIS', Text, quote=False)
    SplitId = Column('SplitId', Text, quote=False)
    AutoClassificationTime = Column('AutoClassificationTime', DateTime, quote=False)

    # theme
    ThematicAutoclassificationIDLevel1 = Column('ThematicAutoclassificationIDLevel1', Text, quote=False)
    ThematicAutoclassificationIDLevel2 = Column('ThematicAutoclassificationIDLevel2', Text, quote=False)
    ThematicAutoclassificationIDLevel3 = Column('ThematicAutoclassificationIDLevel3', Text, quote=False)
    ThematicAutoclassificationIDLevel4 = Column('ThematicAutoclassificationIDLevel4', Text, quote=False)

    # phrase
    Role = Column('Role', Text, quote=False)
    PhraseTime = Column('PhraseTime', DateTime, quote=False)
    PhraseText = Column('PhraseText', Text, quote=False)

    PhraseTextId = Column('PhraseTextId', BigInteger, quote=False, primary_key=True, autoincrement=True)
    TranscriptionType = Column('TranscriptionType', Enum(TranscriptionTypeValues), quote=False)
    ActivityId = Column('ActivityId', Text, quote=False)
    InteractionId = Column('InteractionId', Text, quote=False)
    ThematicOperatorIDLevel1 = Column('ThematicOperatorIDLevel1', Text, quote=False)
    ThematicOperatorIDLevel2 = Column('ThematicOperatorIDLevel2', Text, quote=False)
    ThematicOperatorIDLevel3 = Column('ThematicOperatorIDLevel3', Text, quote=False)
    ThematicOperatorIDLevel4 = Column('ThematicOperatorIDLevel4', Text, quote=False)
    TechThemeList = Column('TechThemeList', ARRAY(Text), quote=False)
    AutoClassificationFl = Column('AutoClassificationFl', Boolean, quote=False)


class AutoclassDialog(Base):
    __tablename__ = 'dialog'
    __table_args__ = {'schema': 'autoclass'}
    DialogId = Column('DialogId', BigInteger, quote=False, primary_key=True, autoincrement=True)
    EDUID = Column('EDUID', Text, quote=False)
    UCID = Column('UCID', Text, quote=False)
    CreationTime = Column('CreationTime', DateTime, quote=False)
    ClientNumber = Column('ClientNumber', Text, quote=False)
    OperatorDNIS = Column('OperatorDNIS', Text, quote=False)
    Duration = Column('Duration', Float, quote=False)
    PhraseCount = Column('PhraseCount', INT, quote=False)
    TranscriptionType = Column('TranscriptionType', Enum(TranscriptionTypeValues), quote=False)
    SplitId = Column('SplitId', Text, quote=False)
    AutoClassificationTime = Column('AutoClassificationTime', DateTime, quote=False)


class AutoclassTheme(Base):
    __tablename__ = 'theme'
    __table_args__ = {'schema': 'autoclass'}
    ThemeId = Column('ThemeId', BigInteger, quote=False, primary_key=True, autoincrement=True)
    DialogId = Column('DialogId', BigInteger,
                      ForeignKey(AutoclassDialog.DialogId, ondelete='CASCADE'),
                      quote=False)
    EDUID = Column('EDUID', Text, quote=False)
    UCID = Column('UCID', Text, quote=False)
    ThematicAutoclassificationIDLevel1 = Column('ThematicAutoclassificationIDLevel1', Text, quote=False)
    ThematicAutoclassificationIDLevel2 = Column('ThematicAutoclassificationIDLevel2', Text, quote=False)
    ThematicAutoclassificationIDLevel3 = Column('ThematicAutoclassificationIDLevel3', Text, quote=False)
    ThematicAutoclassificationIDLevel4 = Column('ThematicAutoclassificationIDLevel4', Text, quote=False)


class AutoclassPhrase(Base):
    __tablename__ = 'phrase'
    __table_args__ = {'schema': 'autoclass'}
    PhraseId = Column('PhraseId', BigInteger, quote=False, primary_key=True, autoincrement=True)
    DialogId = Column('DialogId', BigInteger,
                      ForeignKey(AutoclassDialog.DialogId, ondelete='CASCADE'),
                      quote=False)
    EDUID = Column('EDUID', Text, quote=False)
    UCID = Column('UCID', Text, quote=False)
    Role = Column('Role', Text, quote=False)
    PhraseTime = Column('PhraseTime', DateTime, quote=False)
    PhraseText = Column('PhraseText', Text, quote=False)


class DataGenerator:

    def __init__(self, db_name: str, psswd: str, host: str, port: int):
        self._engine = create_engine(f'postgresql://{db_name}:{psswd}@{host}:{port}', echo=False)
        self._engine.connect()

        self._session = Session(bind=self._engine)

        try:
            self._engine.execute(CreateSchema('autoclass'))
        except ProgrammingError:
            print('Error while creating autoclass schema - perhaps it already exists. Skipping...')
        except Exception:
            raise

    def drop_all_tables(self):
        Base.metadata.drop_all(self._engine)

    def create_all_tables(self):
        Base.metadata.create_all(self._engine)

    def drop_and_create_all_tables(self):
        self.drop_all_tables()
        self.create_all_tables()

    def print_current_tables_size(self, tables_dict: Dict):
        print('\nCurrent size of tables: ', end='')
        for table_name, table_class in tables_dict.items():
            print(f'{table_name} {self._session.query(table_class).count()}', end=' ')
        print()

    @staticmethod
    def generate_rand_date(depth: int = 60):
        return datetime.today() - timedelta(days=random.randint(0, depth))

    @staticmethod
    def generate_autoclass_data() -> Autoclass:
        """
        Generates Autoclass data object to add later to database
        :return: Autoclass object
        """

        return Autoclass(
            EDUID=random.getrandbits(50),
            UCID=random.getrandbits(50),
            CreationTime=DataGenerator.generate_rand_date(),
            ClientNumber=random.randint(70000000000, 79999999999),
            OperatorDNIS=random.randint(1111, 9999),
            Role=random.choice(('Agent', 'Client')),
            PhraseTime=datetime.now(),
            TranscriptionType=random.choice(('Online', 'Offline')),
            PhraseText='text',
            ActivityId=random.getrandbits(50),
            InteractionId=random.getrandbits(50),
            SplitId=random.randint(1111, 9999),
            ThematicOperatorIDLevel1='1',
            ThematicOperatorIDLevel2='2',
            ThematicOperatorIDLevel3='3',
            ThematicOperatorIDLevel4='4',
            ThematicAutoclassificationIDLevel1='01',
            ThematicAutoclassificationIDLevel2='02',
            ThematicAutoclassificationIDLevel3='03',
            ThematicAutoclassificationIDLevel4='04',
            AutoClassificationFl=random.choice((True, False)),
            AutoClassificationTime=datetime.now(),
            PhraseTextId=random.getrandbits(50)
        )

    @staticmethod
    def generate_dialog_data(dialog_id: int) -> AutoclassDialog:
        return AutoclassDialog(
            DialogId=dialog_id,
            EDUID=random.getrandbits(50),
            UCID=random.getrandbits(50),
            CreationTime=DataGenerator.generate_rand_date(),
            ClientNumber=random.randint(70000000000, 79999999999),
            OperatorDNIS=random.randint(1111, 9999),
            Duration=random.uniform(0, 10),
            PhraseCount=random.randint(1, 50),
            TranscriptionType=random.choice(('Online', 'Offline')),
            SplitId=random.randint(1111, 9999),
            AutoClassificationTime=datetime.now()
        )

    @staticmethod
    def generate_theme_data(dialog_id: int) -> AutoclassTheme:
        return AutoclassTheme(
            ThemeId=random.getrandbits(50),
            DialogId=dialog_id,
            EDUID=random.getrandbits(50),
            UCID=random.getrandbits(50),
            ThematicAutoclassificationIDLevel1='01',
            ThematicAutoclassificationIDLevel2='02',
            ThematicAutoclassificationIDLevel3='03',
            ThematicAutoclassificationIDLevel4='04'
        )

    @staticmethod
    def generate_phrase_data(dialog_id: int) -> AutoclassPhrase:
        return AutoclassPhrase(
            PhraseId=random.getrandbits(50),
            DialogId=dialog_id,
            EDUID=random.getrandbits(50),
            UCID=random.getrandbits(50),
            Role=random.choice(('Agent', 'Client')),
            PhraseTime=datetime.now(),
            PhraseText='text'
        )

    def create_data(
            self,
            calls_to_create: int = 100,
            phrases_per_dialog: int = 50,
            themes_per_dialog: int = 5
    ) -> None:

        for _ in range(calls_to_create):

            self._session.add(DataGenerator.generate_autoclass_data())

            dialog_id = random.getrandbits(50)
            self._session.add(DataGenerator.generate_dialog_data(dialog_id=dialog_id))

            for _ in range(phrases_per_dialog):
                self._session.add(DataGenerator.generate_phrase_data(dialog_id=dialog_id))

            for _ in range(themes_per_dialog):
                self._session.add(DataGenerator.generate_theme_data(dialog_id=dialog_id))

        self._session.commit()

    def close_session(self):
        self._session.close()



