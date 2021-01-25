#!/usr/bin/env python
# coding: utf-8

# In[1]:


import cache_magic

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


# In[2]:


# Let`s import all packages that we may need:

import sys 
import tensorflow as tf
import numpy as np # linear algebra
from scipy.stats import randint
import matplotlib.pyplot as plt
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv), data manipulation as in SQL
import matplotlib.pyplot as plt # this is used for the plot the graph 
import seaborn as sns # used for plot interactive graph. 
from sklearn.model_selection import train_test_split # to split the data into two parts
from sklearn.model_selection import KFold # use for cross validation
from sklearn.preprocessing import StandardScaler # for normalization
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline # pipeline making
from sklearn.model_selection import cross_val_score
from sklearn.feature_selection import SelectFromModel
from sklearn import metrics # for the check the error and accuracy of the model
from sklearn.metrics import mean_squared_error,r2_score

## for Deep-learing:
import keras
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.optimizers import SGD 
from tensorflow.keras.callbacks import EarlyStopping
import itertools
from tensorflow.keras.layers import LSTM, Conv1D
from tensorflow.keras.layers import Dropout


from pylab import rcParams
from matplotlib import rc
from kafka import KafkaProducer
import tensorflow_io as tfio
from pandas.plotting import register_matplotlib_converters
from ksql import KSQLAPI


# In[3]:


get_ipython().run_line_magic('matplotlib', 'inline')
get_ipython().run_line_magic('config', "InlineBackend.figure_format='retina'")

register_matplotlib_converters()
sns.set(style='whitegrid', palette='muted', font_scale=1.2)

rcParams['figure.figsize'] = 20, 5


# In[4]:


KafkaDf = pd.read_csv('/home/ashima/DeviceStatus1.csv',parse_dates=['Timestamp'], )
KafkaDf = KafkaDf.set_index(['Timestamp'])
KafkaDf= KafkaDf.groupby('DeviceName').resample('1T', closed='right').mean()
KafkaDf.dropna()


# In[5]:


KafkaDf=KafkaDf.pivot_table(values='powerConsumption',index='Timestamp',columns='DeviceName')
KafkaDf


# In[8]:


# specify columns to plot
import seaborn as sns 
import itertools

cols = [0]
i = 1
groups=cols
values = KafkaDf.values
palette = itertools.cycle(sns.color_palette())

# plot each column
plt.figure(figsize=(10, 10))

for group in groups:
    plt.subplot(len(cols), 1, i)
    c = next(palette)
    plt.plot(values[:, group], marker='o',  color =c)
    plt.title(KafkaDf.columns[group], y=0.75, loc='center',color = 'black')
    i += 1
plt.show()


# In[9]:


plt.plot(KafkaDf['TV'], label='Television')
plt.ylim([0, 50])
plt.legend()


# In[11]:


KafkaDf


# In[12]:


NUM_COLUMNS = len(KafkaDf.columns)
train_size = int(len(KafkaDf) * 0.90)
test_size = len(KafkaDf) - train_size
train, test = KafkaDf.iloc[0:train_size], KafkaDf.iloc[train_size:len(KafkaDf)]
print(train.shape, test.shape)


# In[15]:


from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
scaler = scaler.fit(train[['TV']])

train['TV'] = scaler.transform(train[['TV']])
test['TV'] = scaler.transform(test[['TV']])


# In[16]:


def create_dataset(X, y, time_steps=1):
    Xs, ys = [], []
    for i in range(len(X) - time_steps):
        v = X.iloc[i:(i + time_steps)].values
        Xs.append(v)
        ys.append(y.iloc[i + time_steps])
    return np.array(Xs), np.array(ys)


# In[17]:


TIME_STEPS = 2
X_train, y_train = create_dataset(train[['TV']], train.TV, TIME_STEPS)
X_test, y_test = create_dataset(test[['TV']], test.TV, TIME_STEPS)


# In[20]:


import numpy as np

def to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]


# In[21]:


def error_callback(exc):
    raise Exception('Error while sendig data to kafka: {0}'.format(str(exc)))

def write_to_kafka(topic_name, items):
  count=0
  producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
  for message, key in items:
    string_message = to_str(message)
    string_key = to_str(key)
    producer.send(topic_name, key=string_key.encode('utf-8'), value=string_message.encode('utf-8')).add_errback(error_callback)
    count+=1
  producer.flush()
  print("Wrote {0} messages into topic: {1}".format(count, topic_name))

write_to_kafka("TechDay", zip(X_train, y_train))


# In[22]:


#Convert CSV records to tensors. Each column maps to one tensor

def decode_kafka_item(item):
  print(item.message)
  message = tf.io.decode_csv(item.message,  [[0.0], [0.0]])
  print(message)
  key = tf.strings.to_number(item.key)
  return (message, key)

BATCH_SIZE=5

train_ds = tfio.IODataset.from_kafka('TechDay', partition=0, offset=0)
train_ds = train_ds.map(decode_kafka_item)
print('Pradeep')
train_ds = train_ds.batch(BATCH_SIZE)
print(type(train_ds))


# In[23]:


X_train


# In[25]:


#Reshape Tensors

#Reshape x axis

for elem in train_ds:
    s=tf.shape(elem[0])
    x_train_kafka = tf.reshape(elem[0], shape=[s[0], s[1], 1])
    
#REshape y axis

for elem in train_ds:
    y_train_kafka=elem[1]
  


# In[29]:


# Set the parameters

OPTIMIZER="adam"
LOSS='mse'
EPOCHS=20


# In[30]:


units= 120

model = tf.keras.Sequential()
model.add(tf.keras.layers.Conv1D(filters=32, kernel_size=15, padding='same', activation="linear",input_shape=(2,1)))
model.add(tf.keras.layers.LSTM(units=units,activation="tanh",return_sequences=False))
model.add(tf.keras.layers.Dropout(rate=0.2))
model.add(tf.keras.layers.RepeatVector(n=2))
model.add(tf.keras.layers.LSTM(units=units, activation="tanh", return_sequences=True))
model.add(tf.keras.layers.Dropout(rate=0.2))
model.add(tf.keras.layers.TimeDistributed(tf.keras.layers.Dense(units=1)))
model.compile(loss='mse', optimizer=tf.keras.optimizers.Adam(learning_rate=0.001))
print(model.summary())


# In[32]:


# compile the model
model.compile(optimizer=OPTIMIZER, loss=LOSS, metrics='accuracy')

es = EarlyStopping(monitor='val_loss', mode='min', patience= 25)

# fit the model
history = model.fit(x_train_kafka,y_train_kafka, 
                    epochs=10, batch_size=64,
                    validation_split=0.10,
                    shuffle=False,verbose=1, callbacks=[es])

plt.plot(history.history['loss'], label='train')
plt.plot(history.history['val_loss'], label='validation')
plt.legend();


# In[59]:


def decode_kafka_item(item):
  message = tf.io.decode_csv(item.message, [[0.0] for i in range(NUM_COLUMNS)])
  key = tf.strings.to_number(item.key)
  return (message, key)

BATCH_SIZE=16
SHUFFLE_BUFFER_SIZE=16
test_ds = tfio.experimental.streaming.KafkaGroupIODataset(
    topics=["susy-test"],
    group_id="testcg",
    servers="127.0.0.1:9092",
    configuration=[
        "session.timeout.ms=7000",
        "max.poll.interval.ms=8000",
    ],
)

test_ds = test_ds.map(decode_kafka_item)
test_ds = test_ds.batch(BATCH_SIZE)


# In[21]:


X_train_pred = model.predict(X_train)

train_mae_loss = np.mean(np.abs(X_train_pred - X_train), axis=1)
sns.distplot(train_mae_loss, bins=50, kde=True);


# In[22]:


##Testing/Inference
X_test_pred = model.predict(X_test)
test_mae_loss = np.mean(np.abs(X_test_pred - X_test), axis=1)

THRESHOLD =1
test_score_df = pd.DataFrame(index=test[TIME_STEPS:].index)
test_score_df['loss'] = test_mae_loss
test_score_df['threshold'] = THRESHOLD
test_score_df['anomaly'] = test_score_df.loss > test_score_df.threshold
test_score_df['TV'] = test[TIME_STEPS:].TV


plt.plot(test_score_df.index, test_score_df.loss, label='loss')
plt.plot(test_score_df.index, test_score_df.threshold, label='threshold')
plt.xticks(rotation=25)
plt.legend();

anomalies = test_score_df[test_score_df.anomaly == True]
anomalies.head()
print('Total no of Test Samples:', len(X_test))
print()
print('Total number of anomalies:',len(anomalies))


# In[25]:


plt.plot(
  test[TIME_STEPS:].index, 
  scaler.inverse_transform(test[TIME_STEPS:].TV), 
  label='power Consumption of TV'
);

sns.scatterplot(
  anomalies.index,
  scaler.inverse_transform(anomalies.TV),
  color=sns.color_palette()[3],
  s=52,
  label='anomaly')

plt.xticks(rotation=25)
plt.legend(loc='upper right');


# In[225]:


# make a prediction

yhat = model.predict(test_X)
test_X = test_X.reshape((test_X.shape[0], 7))

# invert scaling for forecast
inv_yhat = np.concatenate((yhat, test_X[:, -6:]), axis=1)
inv_yhat = scaler.inverse_transform(inv_yhat)

inv_yhat = inv_yhat[:,0]

# invert scaling for actual
test_y = test_y.reshape((len(test_y), 1))
inv_y = np.concatenate((test_y, test_X[:, -6:]), axis=1)
inv_y = scaler.inverse_transform(inv_y)
inv_y = inv_y[:,0]

# calculate RMSE
rmse = np.sqrt(mean_squared_error(inv_y, inv_yhat))
print('Test RMSE: %.3f' % rmse)


# In[ ]:




