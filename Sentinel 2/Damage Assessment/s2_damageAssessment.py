# Overview of the Processing Pipeline
'''
Processing Pipeline A -> Read - Resample - Subset - Collocate - Sea Mask - Band Maths - Export (Write)
'''

# Import Relevant Modules
import datetime
import os
from snappy import ProductIO, HashMap, WKTReader, GPF, jpy
import shapefile
import pygeoif

# Define Helper Functions
def getFilePaths(data_dir):
    files = []
    for f in os.listdir(data_dir):
        if f.endswith(".zip"):
            files.append(os.path.join(data_dir, f))
    return files


def getS2AquisitionDate(filePath):
    date = filePath.split('\\')[-1].split('.')[0].split('_')[-1].split('T')[0]
    return int(date)


def identifyMasterSlaveFiles(data_dir):
    filePaths = getFilePaths(data_dir)
    dates = []
    for fPath in filePaths:
        dates.append(getS2AquisitionDate(fPath))
    print(max(dates))
    master = [fn for fn in filePaths if str(min(dates)) in fn]
    slave = [fn for fn in filePaths if str(max(dates)) in fn]
    return master[0], slave[0]

# Define Processing Functions  
def read(filename):
    return ProductIO.readProduct(filename)


def resample(inFile, referenceBand):
    parameters = HashMap()
    parameters.put('referenceBand', referenceBand)
    parameters.put('upsampling', 'Bilinear')
    parameters.put('downsampling', 'First')
    parameters.put('flagDownsampling', 'First')
    parameters.put('resampleOnPyramidLevels', True)
    return GPF.createProduct('Resample', parameters, inFile)


def subset(inFile, shpPath):
    shp_file = shapefile.Reader(shpPath)
    g = []
    for s in shp_file.shapes():
        g.append(pygeoif.geometry.as_shape(s))
    m = pygeoif.MultiPoint(g)
    wkt = str(m.wkt).replace("MULTIPOINT", "POLYGON(") + ")"
    geometry = WKTReader().read(wkt)

    parameters = HashMap()
    parameters.put("copyMetadata", True)
    parameters.put("geoRegion", geometry)
    return GPF.createProduct("Subset", parameters, inFile)


def collocate(masterFile, slaveFile):
    sourceProducts = HashMap()
    sourceProducts.put('master', masterFile)
    sourceProducts.put('slave', slaveFile)
    parameters = HashMap()
    parameters.put('targetProductType', 'COLLOCATED')
    parameters.put('renameMasterComponents', True)
    parameters.put('renameSlaveComponents', True)
    parameters.put('masterComponentPattern', '${ORIGINAL_NAME}_M')
    parameters.put('slaveComponentPattern', '${ORIGINAL_NAME}_S')
    parameters.put('resamplingType', 'CUBIC_CONVOLUTION')
    return GPF.createProduct("Collocate", parameters, sourceProducts)


def seaMask(inFile):
    sourceBands = HashMap()
    sourceBands.put('master', 'B8_M')
    sourceBands.put('slave', 'B8_S')
    parameters = HashMap()
    parameters.put('landMask', False)
    parameters.put('useSRTM', True)
    parameters.put('invertGeometry', False)
    parameters.put('shorelineExtension', 0)
    return GPF.createProduct("Land-Sea-Mask", parameters, inFile)


def BandMaths(inFile, BandName, Expression):
    parameters = HashMap()
    band_descriptor = jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
    target_band = band_descriptor()
    target_band.name = BandName
    target_band.type = 'float32'
    target_band.expression = Expression
    target_bands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
    target_bands[0] = target_band
    parameters.put('targetBands', target_bands)
    return GPF.createProduct('BandMaths', parameters, inFile)

def write(inFile, outFilePath, format=None):
    return ProductIO.writeProduct(inFile, outFilePath, format if format else "BEAM-DIMAP")
    # Allowed formats to write: GeoTIFF-BigTIFF,HDF5,Snaphu,BEAM-DIMAP,
    # GeoTIFF+XML,PolSARPro,NetCDF-CF,NetCDF-BEAM,ENVI,JP2,
    # Generic Binary BSQ,Gamma,CSV,NetCDF4-CF,GeoTIFF,NetCDF4-BEAM

# Set directory paths
path_S2_data = 'E:\Work\Independent Projects\Damage Assessment Israel-Gaza Conflict 2021\Data\Sentinel 2'
processing_dir = "E:\Work\Independent Projects\Damage Assessment Israel-Gaza Conflict 2021\Processed\S2"
# Set shapefile path
gazaDistrictExtent_shp = r"E:\\Work\\Independent Projects\\Damage Assessment Israel-Gaza Conflict 2021\\Data\\Shapefile\\GazaDistrictExtent.shp"

# Define Processing Pipeline
if __name__ == '__main__':
    start = datetime.datetime.now()
    print(f'Time Started: {start}')

    # Assign Images that represent before (master) and post (slave) conditions
    masterFilePath, slaveFilePath = identifyMasterSlaveFiles(path_S2_data)

    # Process Master
    master = read(masterFilePath)
    master_resample = resample(master, 'B2')
    master_resample_subset = subset(master_resample, gazaDistrictExtent_shp)

    # Process Slave
    slave = read(slaveFilePath)
    slave_resample = resample(slave, 'B2')
    slave_resample_subset = subset(slave_resample, gazaDistrictExtent_shp)

    # Collocate
    c = collocate(master_resample_subset, slave_resample_subset)

    # Land/Sea Mask
    c_seaMask = seaMask(c)
    print(list(c_seaMask.getBandNames()))

    # Band Maths: Change Detection
    s2_change = BandMaths(c_seaMask, "S2_Change", "B8_M - B8_S")
    # print(list(s2_change.getBandNames())) # Check bands

    # Export as GeoTIFF
    m_date = master.getName().split('_')[-1].split('T')[0]
    s_date = slave.getName().split('_')[-1].split('T')[0]
    outFileName = 'S2_{}_{}_collocate_msk'.format(m_date, s_date)
    outPath = os.path.join(processing_dir, outFileName)

    print(outPath)
    write(s2_change, outPath, format='GeoTIFF')

    print(f'Time Taken: {datetime.datetime.now() - start}')
