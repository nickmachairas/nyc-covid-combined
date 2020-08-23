# -- IMPORTS ------------------------------------------------------------------
import os
import urllib.request
from datetime import datetime
import pandas as pd
from tqdm import tqdm


# -- VARS ---------------------------------------------------------------------
FILE_DIR = 'data_files'
DATA_DIR = 'data-by-modzcta'
TESTS_DIR = 'tests-by-zcta'
DATA_FILE = 'data-by-modzcta.csv'
TESTS_FILE = 'tests-by-zcta.csv'
RAW_URL = 'https://raw.githubusercontent.com/nychealth/coronavirus-data/'


# -- COMMITS ------------------------------------------------------------------
data_commits = {
    '2020-08-22': '7e3f387baff20f9cd051a0795a7872f0ab769dcf',
    '2020-08-21': 'ce3f3a6952914b8222b3f495ccd69a7f3b881e45',
    '2020-08-20': '647a8213dc537c9fad89162aaf7efd4d77f4a80f',
    '2020-08-19': '5d654516f35cff33c071f702ba727e0ac1b626e4',
    '2020-08-18': '02b25780056aa1c24d29f307d76d0f188a1b9839',
    '2020-08-17': '0f4f0056ee6de6f2a54b0272c7638f5de46698d2',
    '2020-08-16': 'a5fecbe9f995723e5a0c4b816a985eaff4789340',
    '2020-08-15': '91bd862d71e2d6df81843a5e05510629884ca4d2',
    '2020-08-14': '538891f0e2aeeb60b543bb930ea35b6d08f72790',
    '2020-08-13': '5f06bd065ec344111d569b23f29f42e0701eac01',
    '2020-08-12': '16224c08aeaf5a625785ac4a82137d0369db4e93',
    '2020-08-11': '5158a16b1c0927a55e08f9d2a6c94279d4a90003',
    '2020-08-10': 'f4f433546d1c2cd2321ac5d3bb51d8689e83d419',
    '2020-08-09': '66105d19d9d1612cba57d904cd90eebde9115704',
    '2020-08-08': '7c5b81b991c2b5edbf26c237286d497bf869be2c',
    '2020-08-07': 'dcdc45973712d6bc15523afaecd27d37d6da305e',
    '2020-08-06': 'cf02bdfd9dfcf6d5e5d75b9b138b6e727335e9ad',
    '2020-08-05': 'b6a811edae7804c31e18e94c8602a73e2938a191',
    '2020-08-04': '2f12509de24f10a5bdccc02bb79d1ad85538d8aa',
    '2020-08-03': '773ffb03e3e4efd32b855f3e31cb113c9e820273',
    '2020-08-02': 'a7315f625e7ef4eb15ed326d8ef66a703fccb613',
    '2020-08-01': '6a26616a502ae5d02e261262ee155fc20041ee30',
    '2020-07-31': 'f10021ab793108aad8a25db7a85fa0d722ad7912',
    '2020-07-30': '2bed49841afdd9a9c6b584673b03a901a6ad2140',
    '2020-07-29': '18b56f1b5a1dc01fcde42a3243e5102bf8935fae',
    '2020-07-28': '6daff424410b0e9faef3f33c8c656c5732282f85',
    '2020-07-27': 'ec4a60ebb5eb79970161ff6ac925eaec481ae6a2',
    '2020-07-26': 'f7f16ad2ec56552acaa750825238cfb98796cc96',
    '2020-07-25': 'fa0817c7d262dce6ebb2efc7d50bc83bf41991ed',
    '2020-07-24': 'c2539dc70437a42575f3c3ea8b230047de23dff3',
    '2020-07-23': '4270c108e635c67d80af970c4540abdbb003b234',
    '2020-07-22': '6e4171f60a90d5ee309111facbaa278cccb82a86',
    '2020-07-21': 'ef6900beea4eac85128cd117dc83d1319191de08',
    '2020-07-20': '4613dc2bca155e3136d2e204ad89d64a30f7a334',
    '2020-07-19': 'b1a629db5ae84d7277e532b203d655e9532720bb',
    '2020-07-18': '85349ab6bd47414e436873afd7fba6a45bb6ac12',
    '2020-07-17': '732da3899c7995be809c7034d155e643b8234fde',
    '2020-07-16': '884471b3d7fb96a85a500e7360c3eb3d4593cdf4',
    '2020-07-15': 'd9bee1e514769766bfc888c5cf5b56ae7d2fe717',
    '2020-07-14': 'd3060c1cb1d3afddef722b42c6aa683fb90458e3',
    '2020-07-13': 'f9726f4702d598be69987465de64c516a797fec7',
    '2020-07-12': '4f81f2d5e1b6bcd3f01267ffac4259cda7fae689',
    '2020-07-11': '7ecf2cdd35fe3a7a0c3d4129a9888fd4ca8550ed',
    '2020-07-10': '70cb39a0b8b78433d26dc8f816d61da78fee75f5',
    '2020-07-09': 'ce5be7225940034f774ab31d0dc09ba54125642f',
    '2020-07-08': '17e76119fc87a656c9ef3729e5e9ac702894b40b',
    '2020-07-07': '7132770488c7ef820ab3c3bc9dd22a3dbe5572fe',
    '2020-07-06': 'eda5f92d1396398f7c0abb72c7d0ce189a374281',
    '2020-07-05': '2232fd908e3e32244bf653e51e7684af9c87970b',
    '2020-07-04': '72af70695f62b928425560ff002edc372141ca81',
    '2020-07-03': '97b6723d9a02b5f3108b3864ea52627dee92ce68',
    '2020-07-02': '144c474d7b7cb2d2fd1eb7a0c39208453519e311',
    '2020-07-01': '41f823d90887251735ad4e6f4814548fc9526606',
    '2020-06-30': '695e5fbdada25273f13b4af024f96e37f8503efc',
    '2020-06-29': None,
    '2020-06-28': 'f78cda145befd762b6644b2f57fa47bbc9ce92e3',
    '2020-06-27': '5d87847b9fd6da73ff5c1ba03dada0b624b60744',
    '2020-06-26': '7e1c437a15155923cdd3661efe3d363feb934be0',
    '2020-06-25': '296486db8a787e8da20e73bae33f7705e01fbb6b',
    '2020-06-24': 'be46b4e48653c29fab890473672cd7d7e0c7e59c',
    '2020-06-23': 'cf508e8fe08ddff44a847c1b54209c598b88c913',
    '2020-06-22': 'cc87cd44bbae0b365e7ec83d25f9ad5bc6b72778',
    '2020-06-21': 'deac492572f142ff768b0f377798512b7d71319a',
    '2020-06-20': '1e719a7f7e0db540be277d2f6cf18badc3088ee9',
    '2020-06-19': '7e6195efe0a081ecdf33a7f2ec003d9230268c12',
    '2020-06-18': 'ef7b0a53f3f2007fbc98d201a0bec962af06af42',
    '2020-06-17': '9139cf24bd5501cbdff732f4738930ebf2d21ee4',
    '2020-06-16': 'a86f0bf54cf761a0756e99d24fd037c04cb0a2ec',
    '2020-06-15': '8ebc7a35f37f2ed2310574ea5b2e4047163c8265',
    '2020-06-14': '9913aec646fd3eb47b9a84afaf4db3f231b0c3a9',
    '2020-06-13': '9b7a380c44801db4cf3fdd87fe97747542e20e03',
    '2020-06-12': 'b92f6e5cd8d625fba712128a54da8236694c5941',
    '2020-06-11': 'b6ae2b94fb0f32283201433147a6f07d0aa14815',
    '2020-06-10': 'b820b687f7b032763fbb38b59d79f512dc3971df',
    '2020-06-09': '5ecc5d16eeb6f8202c3457bea892cfe8eea5dab9',
    '2020-06-08': '3f21405a00bf7c93db5a5209b44886882feb30a6',
    '2020-06-07': '6328e0b130a4dee1f6140ed3aa8264168d2a925f',
    '2020-06-06': '9e0c1fe569b09ebddbccd6c9f6d519e28920e86a',
    '2020-06-05': '582977c6b9e14f048305403e1b830145e0669a5e',
    '2020-06-04': 'f094fb20c7843f218cea3bc47a06ebe8055d6127',
    '2020-06-03': 'eb3b8e99cb07bb04ff7b73c2d86e45ae68a4aab7',
    '2020-06-02': '53f5d7935feb50fe87132c928f45455af4149407',
    '2020-06-01': '62444c1cbfda69c28ed468d14be3f332ea35eea7',
    '2020-05-31': '9b5cd4d9259587d41017804dda6589a7b2ab5b3c',
    '2020-05-30': '3e9a27ce96a59d9107295ade2b2d7255e605b47b',
    '2020-05-29': '8636c5557e93e0c5f35b8c2f8ae1ed1e1077d469',
    '2020-05-28': '65efb1f75b2714bf06fc2e76fba22315727f6943',
    '2020-05-27': '498a068d6ca1765231b8526a5529ec552a0daff7',
    '2020-05-26': 'd52fdfe48dc9b8fe446df048c681b9c19555e469',
    '2020-05-25': 'f19c0bc29137b7a1c503c09ec0c2b2e08d27d439',
    '2020-05-24': '9332798c68db780365109f156892efc3d9315485',
    '2020-05-23': '8d88b2c06cf6b65676d58b28979731faa10c193c',
    '2020-05-22': '3cbb3b744ed90befb37ea7680fac3aa7f782bb30',
    '2020-05-21': 'd3a18736ecd9589ab4db24f8cdf76fd40b82c661',
    '2020-05-20': '0c4a03c9a39470b49a60eb1456c64fb0df9e08f8',
    '2020-05-19': 'a68f42fe3615eaf3d36df219059eb94595cee933',
    '2020-05-18': '50e60ee5c8f36198abc9b697761128dd29f10fc9',
}

tests_commits = {
    '2020-06-08': '3f21405a00bf7c93db5a5209b44886882feb30a6',
    '2020-06-07': '6328e0b130a4dee1f6140ed3aa8264168d2a925f',
    '2020-06-06': '9e0c1fe569b09ebddbccd6c9f6d519e28920e86a',
    '2020-06-05': '582977c6b9e14f048305403e1b830145e0669a5e',
    '2020-06-04': 'f094fb20c7843f218cea3bc47a06ebe8055d6127',
    '2020-06-03': None,
    '2020-06-02': '53f5d7935feb50fe87132c928f45455af4149407',
    '2020-06-01': '62444c1cbfda69c28ed468d14be3f332ea35eea7',
    '2020-05-31': '9b5cd4d9259587d41017804dda6589a7b2ab5b3c',
    '2020-05-30': '3e9a27ce96a59d9107295ade2b2d7255e605b47b',
    '2020-05-29': '8636c5557e93e0c5f35b8c2f8ae1ed1e1077d469',
    '2020-05-28': '65efb1f75b2714bf06fc2e76fba22315727f6943',
    '2020-05-27': '498a068d6ca1765231b8526a5529ec552a0daff7',
    '2020-05-26': 'd52fdfe48dc9b8fe446df048c681b9c19555e469',
    '2020-05-25': 'ea689d3d3eb427c4ae9cf1855836b4ceee8f6dab',
    '2020-05-24': '79d57e636ce99e8525059ed2c8769a04afe39fac',
    '2020-05-23': '8d88b2c06cf6b65676d58b28979731faa10c193c',
    '2020-05-22': '3cbb3b744ed90befb37ea7680fac3aa7f782bb30',
    '2020-05-21': 'c5cb7417b5ecb836f9727d6996f97d3cd050ba8d',
    '2020-05-20': None,
    '2020-05-19': 'a68f42fe3615eaf3d36df219059eb94595cee933',
    '2020-05-18': '80df5dcbc293e03fe7af61683f4f6d3c1b940d5d',
    '2020-05-17': 'd9d894ac238333fb90ff44f19ad6bc7ad227eda4',
    '2020-05-16': '25021ca2c2f77589988e750a4024967fa887c85d',
    '2020-05-15': 'ebe9ba75c55a2f1dcedbfc00396a6dc0873b71c4',
    '2020-05-14': 'ac5cfc575709dfe8a928a7637e4d5c4ccf33d43d',
    '2020-05-13': '82ea643906d764faae2da4f9fdd1cdd922d98c73',
    '2020-05-12': '6b501e5e4fbe09d49d454e00e18205dd6fec9bff',
    '2020-05-11': '5ef81d9009d68a65ef56a27ed2b2b7506952181a',
    '2020-05-10': '257399da1e7e974e9366bf4c5d1d3bfbc6dc9093',
    '2020-05-09': '16f92651e3cd6585df1165e74126258f59f6fb91',
    '2020-05-08': '2dad8ad0c34db78a343a304ee5bcd56cf2a2194b',
    '2020-05-07': '6d7c4a94d6472a9ffc061166d099a4e5d89cd3e3',
    '2020-05-06': 'd1df026ad726baf9554f3edd246b7495a8cd247f',
    '2020-05-05': 'dbf813f886c2323ac1e161b9aac966e87278ae80',
    '2020-05-04': 'e9647e089c58884bb8ee5dd4972782c69327e3ee',
    '2020-05-03': '26d8d4c6a64c75608eb9229a41cffbb72a03685d',
    '2020-05-02': '946537b1647b4bc7c621716ef6f8e336de09068b',
    '2020-05-01': '9e26adc2c475d3378d7579e48e936f8a807b254b',
    '2020-04-30': 'cdc5866a09ad9fa3d2c8e449dcf1803775b0a6df',
    '2020-04-29': 'c68db06972c3100297a615ba397245eff8b3ee9f',
    '2020-04-28': '81fcf12be08aadd730d1d39c153e3fd9ab371e77',
    '2020-04-27': '3ec3fa97d44c5b3054477c9c0998fa6d466bca72',
    '2020-04-26': 'da609d813e5a53dd8402ad018bc2a2d1ea7e3170',
    '2020-04-25': '4bc53684bcae8ecaede11197d5f01c2d4c5f956a',
    '2020-04-24': '8f4cd591c65b11c23bc3ebae25f523df5da6ceb6',
    '2020-04-23': '93c29d8395f89d5d0eff39e1a9dcba69c820b33f',
    '2020-04-22': '9147df02c9f9c8eab00719ffeec242088a666203',
    '2020-04-21': 'bbbb0f31f7a6f62b64012e8c0c5cb2d3495e6670',
    '2020-04-20': '0a74f0850087758da3579886ae8b7365e182ed9e',
    '2020-04-19': '498c34f8534bc865b15dd09e9b560bd457ef5b9b',
    '2020-04-18': 'd3a8994716870cfdbd6cc2fb356c31588446fc25',
    '2020-04-17': '21916256325a11aae77bbe69029085f43592f2d1',
    '2020-04-16': 'b08d47482771bd35b3f25106dfc076e0a2649d29',
    '2020-04-15': '6c9e9954c58ddf7556fe52937c487af29bf6ceb4',
    '2020-04-14': 'b2104e26d781f6ebf4eff31afcd93cce887ff79b',
    '2020-04-13': '1dc35df3a8d1c19587cf2cfe72567594ae079650',
    '2020-04-12': 'd34e6aab1e0dd0e0125e74519489e7893d33c9dd',
    '2020-04-11': '8542fbf18049d804eb8de7594123c13e533d1a42',
    '2020-04-10': '3fdd59a195bff5c4473a2086093ed656702d6569',
    '2020-04-09': 'e1f1d9a63fac772e26a45220d3c8199a75938656',
    '2020-04-08': 'e19db289166f73282d39dfcef0d47a324d654c07',
    '2020-04-07': '55495966af131723fdbd1a4357c1f84adea03982',
    '2020-04-06': None,
    '2020-04-05': '98a7fd1c5eccdae11d604dd98b2c4a2eafef059b',
    '2020-04-04': '0ae531d56696b7dfa01d1d1ad6286d7ae03350c7',
    '2020-04-03': '0074809280d3f9ae0bd09ca62629fb21243ffc72',
    '2020-04-02': None,
    '2020-04-01': '097cbd70aa00eb635b17b177bc4546b2fce21895',
}


# -- FILE DOWNLOAD ------------------------------------------------------------
def get_data():
    print('*** Downloading data-by-modzcta ... ***')
    for date in data_commits.keys():
        if data_commits[date]:
            url = RAW_URL + data_commits[date] + '/' + DATA_FILE
            fpath = os.path.join(FILE_DIR, DATA_DIR, date + '_' + DATA_FILE)
            urllib.request.urlretrieve(url, fpath)
            print('Download complete for data-by-modzcta', date)

    print('*** Downloading tests-by-zcta ... ***')
    for date in tests_commits.keys():
        if tests_commits[date]:
            url = RAW_URL + tests_commits[date] + '/' + TESTS_FILE
            fpath = os.path.join(FILE_DIR, TESTS_DIR, date + '_' + TESTS_FILE)
            urllib.request.urlretrieve(url, fpath)
            print('Download complete for tests-by-zcta', date)


# -- FILE READ & COMBINE ------------------------------------------------------
def combine():
    header = 'LAST_UPDATE,MODIFIED_ZCTA,NEIGHBORHOOD_NAME,BOROUGH_GROUP,' + \
        'COVID_CASE_COUNT,COVID_CASE_RATE,POP_DENOMINATOR,' + \
        'COVID_DEATH_COUNT,COVID_DEATH_RATE,PERCENT_POSITIVE,TOTAL_COVID_TESTS'

    data_files = []
    data_path = os.path.join(FILE_DIR, DATA_DIR)
    for (dirpath, dirnames, filenames) in os.walk(data_path):
        data_files.extend(filenames)
        break
    data_files = sorted(data_files, reverse=True)

    test_files = []
    test_path = os.path.join(FILE_DIR, TESTS_DIR)
    for (dirpath, dirnames, filenames) in os.walk(test_path):
        test_files.extend(filenames)
        break

    test_files = sorted([
        f for f in test_files if
        datetime.strptime(f.split('_')[0], '%Y-%m-%d') < datetime(2020, 5, 18)
        ], reverse=True)

    with open('all_combined.csv', 'w') as f:
        f.write(header + '\n')
        for file in data_files:
            date = file.split('_')[0]
            fpath = os.path.join(FILE_DIR, DATA_DIR, file)
            with open(fpath, 'r') as ff:
                ff.readline()
                for line in ff:
                    if len(line.split(',')) < 10:
                        f.write(date + ',' + line.rstrip() + ',\n')
                    else:
                        f.write(date + ',' + line)
        for file in test_files:
            date = file.split('_')[0]
            fpath = os.path.join(FILE_DIR, TESTS_DIR, file)
            with open(fpath, 'r') as ff:
                ff.readline()
                ff.readline()
                for line in ff:
                    vals = line.replace('"', '').rstrip().split(',')
                    if vals[0] != '99999':
                        if len(vals) == 4:
                            f.write(date + ',' + vals[0] + ',,,' + vals[1] +
                                    ',,,,,' + vals[3] + ',' + vals[2] + '\n')
                        else:
                            f.write(date + ',' + vals[0] + ',,,' + vals[1] +
                                    ',,,,,,' + vals[2] + '\n')


# -- CALCULATE DIFFS ----------------------------------------------------------
def diffs():
    df = pd.read_csv('all_combined.csv')
    dates = sorted(df['LAST_UPDATE'].unique())
    zip_codes = sorted(df['MODIFIED_ZCTA'].unique())
    zip_codes.remove(11096)
    zip_diffs = {}
    print('Calculating case diff per day...')
    for zip_code in tqdm(zip_codes):
        zip_diffs[zip_code] = []
        for d1, d2 in zip(dates[:-1], dates[1:]):
            case_d1 = df['COVID_CASE_COUNT'][
                (df.MODIFIED_ZCTA == zip_code) & (df.LAST_UPDATE == d1)].values[0]
            case_d2 = df['COVID_CASE_COUNT'][
                (df.MODIFIED_ZCTA == zip_code) & (df.LAST_UPDATE == d2)].values[0]
            zip_diffs[zip_code].append(case_d2 - case_d1)

    header = 'ZIP_CODE'
    for date in dates[1:]:
        header = header + ',' + date
    with open('case_diff_by_date.csv', 'w') as f:
        f.write(header + '\n')
        for zip_code in zip_diffs.keys():
            line = str(zip_code)
            for val in zip_diffs[zip_code]:
                line = line + ',' + str(val)
            f.write(line + '\n')


# -- RUN ----------------------------------------------------------------------
if __name__ == '__main__':
    get_data()
    combine()
    diffs()
