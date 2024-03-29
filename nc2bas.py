# encode: UTF-8
import os
import glob
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
# import cdsapi


# недописано
def cds2nc(path, vars, years):
    '''

    :return:
    '''

    os.chdir(path)

    c = cdsapi.Client()

    for y in years:
        for v in vars:
            api_request = [
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': ['2m_temperature','total_precipitation','2m_dewpoint_temperature'],
                    'month': [
                        '01', '02', '03',
                        '04', '05', '06',
                        '07', '08', '09',
                        '10', '11', '12',
                    ],
                    'day': [
                        '01', '02', '03',
                        '04', '05', '06',
                        '07', '08', '09',
                        '10', '11', '12',
                        '13', '14', '15',
                        '16', '17', '18',
                        '19', '20', '21',
                        '22', '23', '24',
                        '25', '26', '27',
                        '28', '29', '30',
                        '31',
                    ],
                    'time': [
                        '00:00', '01:00', '02:00',
                        '03:00', '04:00', '05:00',
                        '06:00', '07:00', '08:00',
                        '09:00', '10:00', '11:00',
                        '12:00', '13:00', '14:00',
                        '15:00', '16:00', '17:00',
                        '18:00', '19:00', '20:00',
                        '21:00', '22:00', '23:00',
                    ],
                    'area': [
                        56.7, 96.9, 46.5,
                        113.8,
                    ],
                    'format': 'netcdf',
                },
                'ERA5_' + str(v) + '_' + str(y) + '.nc']
            print(api_request)
            c.retrieve(**api_request)


def nc2bas(path):
    print('Converting %s to *.bas' % path)
    ds = xr.open_dataset(path)
    print(ds)

    # пишем координаты
    # coords = np.array(np.meshgrid(ds.latitude.values, ds.longitude.values)).T.reshape(-1, 2)
    coords = np.array(np.meshgrid(ds.lat.values, ds.lon.values)).T.reshape(-1, 2)
    if not os.path.isfile('MeteoStation.txt'):
        xy_df = pd.DataFrame(data=coords, columns=['lat', 'lon'])
        xy_df.index = xy_df.index + 1
        xy_df.to_csv('MeteoStation.txt')

    # цикл по переменным в файле
    vars = list(ds.data_vars.keys())
    for var in vars:

        # ресэмплинг с часов до суток
        if var == 'tp':
            df = ds[var].resample(time='D').sum().to_dataframe().reset_index()
            df[var] = df[var] * 1000
        elif var == 't2m':
            df = ds[var].resample(time='D').mean().to_dataframe().reset_index()
            df[var] = df[var] - 272.15
        elif var == 'd2m':
            dtd = ds[var].resample(time='D').mean().to_dataframe().reset_index()
            dtd[var] = dtd[var] - 272.15
            dt2 = ds['t2m'].resample(time='D').mean().to_dataframe().reset_index()
            dt2['t2m'] = dt2['t2m'] - 272.15
            # рассчитываем влажность насыщения и абсолютную
            dtd['ea'] = 6.112 * np.exp((17.67 * dtd[var]) / (dtd[var] + 243.5))
            dt2['es'] = 6.112 * np.exp((17.67 * dt2['t2m']) / (dt2['t2m'] + 243.5))
            # рассчитываем дефицит как разницу
            dtd[var] = dt2['es'] - dtd['ea']
            df = dtd[['time', 'latitude', 'longitude', var]]

        print(var)
        # stations = np.repeat(np.arange(len(coords)), len(ds.time.values) / 24)
        stations = [x for x in range(len(coords))] * int(len(ds.time.values) / 24)
        df['stations'] = stations
        df = df.pivot_table(index='time', columns='stations', values=var)

        # делаем файл с правильным названием
        if var == 'tp':  # обработка файлов с осадками
            outfile = 'PRE' + str(df.index.min().year)[-2:] + '.bas'
        elif var == 't2m':
            outfile = 'TEMP' + str(df.index.min().year)[-2:] + '.bas'
        elif var == 'd2m':
            outfile = 'DEF' + str(df.index.min().year)[-2:] + '.bas'


        # пишем в файл
        with open(outfile, 'w') as f:

            # пишем в него хэдер
            if var == 'tp':
                f.write(r'Precipitation, mm' + '\n')
            elif var == 't2m':
                f.write(r'Temperature, oC' + '\n')
            elif var == 'd2m':
                f.write(r'Deficit, hPa' + '\n')
            # количество станций и дней в файле
            f.write(str(len(df.columns)) + ' ' + str(len(df.index)) + '\n')
            # номера всех станций
            f.write(','.join([str(x) for x in df.columns.values]) + '\n')
            # три
            f.write('\n')
            # пустых
            f.write('\n')
            # строки
            f.write('\n')
            # данные, причем в виде форматированной строки и заменяем в них запятую на пробел
            cont = df.to_csv(na_rep=' -99.00', date_format='%Y%m%d', header=False, float_format='%7.2f')
            cont = cont.replace(',', ' ')
            f.write(cont)
            f.close()

    print('Done')

def ewembi2bas(path):
    print('Converting %s to *.bas' % path)
    ds = xr.open_dataset(path)
    # print(ds)

    # пишем координаты
    # coords = np.array(np.meshgrid(ds.latitude.values, ds.longitude.values)).T.reshape(-1, 2)
    coords = np.array(np.meshgrid(ds.lat.values, ds.lon.values)).T.reshape(-1, 2)
    if not os.path.isfile('MeteoStation.txt'):
        xy_df = pd.DataFrame(data=coords, columns=['lat', 'lon'])
        xy_df.index = xy_df.index + 1
        xy_df.to_csv('MeteoStation.txt')

    # цикл по переменным в файле
    vars = list(ds.data_vars.keys())
    for var in vars:

        # ресэмплинг с часов до суток
        if var == 'pr':
            df = ds[var].resample(time='D').sum().to_dataframe().reset_index()
            df[var] = df[var] * 1000
        elif var == 'tas':
            df = ds[var].resample(time='D').mean().to_dataframe().reset_index()
            df[var] = df[var] - 272.15
        elif var == 'hurs':
            dRH = ds[var].resample(time='D').mean().to_dataframe().reset_index()
            ds_tas = xr.open_dataset(path.replace('hurs', 'tas'))
            dt2 = ds_tas['tas'].resample(time='D').mean().to_dataframe().reset_index()
            dt2['tas'] = dt2['tas'] - 272.15
            # # рассчитываем влажность насыщения и абсолютную
            dt2['es'] = 6.112 * np.exp((17.67 * dt2['tas']) / (dt2['tas'] + 243.5))
            dRH['ea'] = (dRH[var] * dt2['es']) / 100
            # # рассчитываем дефицит как разницу
            dRH['def'] = dt2['es'] - dRH['ea']
            df = dRH[['time', 'lat', 'lon', 'def']]
            var = 'def'
            # print(df.head())

        print(var)
        # stations = np.repeat(np.arange(len(coords)), len(ds.time.values) / 24)
        stations = [x for x in range(len(coords))] * int(len(ds.time.values))
        df.loc[:, 'stations'] = stations
        df = df.pivot_table(index='time', columns='stations', values=var)

        # цикл по годам в данных
        yrs = df.index.year.unique()
        for yr in yrs:
            out_df = df.loc[df.index.year == yr, :]
            # print(out_df.head())
            # делаем файл с правильным названием
            if var == 'pr':  # обработка файлов с осадками
                outfile = 'PRE' + str(yr)[-2:] + '.bas'
            elif var == 'tas':
                outfile = 'TEMP' + str(yr)[-2:] + '.bas'
            elif var == 'def':
                outfile = 'DEF' + str(yr)[-2:] + '.bas'


            # пишем в файл
            with open(outfile, 'w') as f:

                # пишем в него хэдер
                if var == 'pr':
                    f.write(r'Precipitation, mm' + '\n')
                elif var == 'tas':
                    f.write(r'Temperature, oC' + '\n')
                elif var == 'def':
                    f.write(r'Deficit, hPa' + '\n')
                # количество станций и дней в файле
                f.write(str(len(out_df.columns)) + ' ' + str(len(out_df.index)) + '\n')
                # номера всех станций
                f.write(','.join([str(x) for x in out_df.columns.values]) + '\n')
                # три
                f.write('\n')
                # пустых
                f.write('\n')
                # строки
                f.write('\n')
                # данные, причем в виде форматированной строки и заменяем в них запятую на пробел
                cont = out_df.to_csv(na_rep=' -99.00', date_format='%Y%m%d', header=False, float_format='%7.2f')
                cont = cont.replace(',', ' ')
                f.write(cont)
                f.close()

    print('Done')

def nc2bas_batch(path):
    os.chdir(path)
    ff = glob.glob("*.nc")
    if not ff:
        print('No files to convert.')
        exit()
    else:
        print("Detected NetCDF files: \n", "\n".join(ff))
        for f in ff:
            # nc2bas(f)
            ewembi2bas(f)

# главный модуль
if __name__ == "__main__":
    # запрос на данные
    # cds2nc(r'd:\EcoMeteo\ERA5\baikal', ['d2m'], range(1979, 2021))

    # конвертация данных
    path = r'D:\YandexDisk\ИВПРАН\крым\данные\ewembi2a_krym'
    nc2bas_batch(path)





