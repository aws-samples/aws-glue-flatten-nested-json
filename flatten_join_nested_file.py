# This AWS Glue job uses the Relationalize transform and some basic level pyspark code to ingest
# an highly nested JSON file, un-nest all the sub-structures and store the result as a set of tables
# in Amzon Redshift or Amazon S3 or both.
# The names of tables and column are cleansed and implified before they are written to the target repository.
# Optionally some or all of the tables can be pre-joined in a more denormalized schema,
# thus limiting the total number of tables produced.


# import libraries definition

from datetime import datetime
import sys
from awsglue.transforms import ResolveChoice, Relationalize, DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# Parameters mapping : retrieve the parameters passed to the Glue job and store them in the array "args"

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'glue_db_name', 'glue_orig_table_name', 's3_temp_folder',
                                     's3_target_path', 'root_table', 'redshift_connection', 'redshift_db_name', 'redshift_schema', 'num_level_to_denormalize', 'num_output_files', 'keep_table_prefix', 'target_repo'])

# Spark and Glue context initialization:

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

# Current default parameters value used in the Glue job definition (change the parameters value at run time or edit the job definition to use your own):

# glue_db_name = 'glue-blog'                                AWS Glue Catalog Database where the source table is stored, the database need to exist in the catalog
# glue_orig_table_name = 'json_source'                      AWS Glue Catalog Table created crawling the source files, the table need to exist in the catalog
# redshift_connection = 'gluersdb'                          AWS Glue Catalog Connection to the redshift Database, the connection need to exist in the catalog
# s3_temp_folder = 's3://fna-glue-blog-useast1/tmp/'        Temporary S3 path used by the glue job when loading Redshift directly, the S3 bucket need to exist the path will be created by the job
# s3_target_path = 's3://fna-glue-blog-useast1/data_lake/'  Target S3 path to be used to store the target table in parquet format, the S3 bucket need to exist the path will be created by the job
# redshift_db_name = 'gluersdb'                             Amazon Redshift database name, the database need to exist
# redshift_schema = 'ny_phil'                               Amazon Redshift schema name, the schema need to exist
# root_table = 'ny_phil_perf_hist'                          Logical name you want to give to the root level of the json structure
# num_level_to_denormalize = 0                              Number of nested levels to denormalize in a single table:
#                                                           0 = no denormalization, is the default;
#                                                           set to the number of nested levels that you want to be denormalized in a single table
#                                                           (if there are less levels than requested a single table will be created)
# num_output_files = 4                                      Number of files to be created when writing to S3, used to avoid creating too many small files;
#                                                           if loading to Redshift should be equal to the number of slices in the cluster
# keep_table_prefix = 0                                     Boolean variable, default is 0 that means that the table name is cleaned of all prefixes
#                                                           set to 1 to keep the parent table name as a prefix for each table (could become very verbose)
# target_repository = 'all'                                 Type of the repository to write the target table, default is 'all', write to S3 and Redshift;
#                                                           set to 's3' to write only to Amazon S3 only
#                                                           set to 'redshift' to write only to Amazon Redshift only
#                                                           set to 'none' to simulate the job and print out the schema of the final tables only

glue_db_name = args['glue_db_name']
glue_orig_table_name = args['glue_orig_table_name']
s3_temp_folder = args['s3_temp_folder']
s3_target_path = args['s3_target_path']
redshift_connection = args['redshift_connection']
redshift_db_name = args['redshift_db_name']
redshift_schema = args['redshift_schema']
root_table = args['root_table']
num_level_to_denormalize = int(args['num_level_to_denormalize'])
num_output_files = int(args['num_output_files'])
keep_table_prefix = args['keep_table_prefix']
target_repository = args['target_repo']

# additional variable inizialization:

dynamicframes_map = {}
dataframes_map = {}
s3_target_path_map = {}
datasink_map = {}
tables_info_map = {}

# variables needed to automatically denormalize the tables

table_count = 0
join_count = 0
number_nested_levels = 0
start_join_level = 0
tables_to_join_map = {}
denormlized_dataframe_map = {}
table_with_nested_level = {}


# helper functions definition:

# Add a prefix to a name
# helper functions definition:

# Add a prefix to a name


def add_prefix(c, prefix):
    if prefix in c:
        r = c
    else:
        r = prefix+'_'+c
    return r

# Remove a recursive suffix from a name


def rm_suffix(name, suffix, lvl):
    if suffix == name[len(name)-len(suffix):len(name)]:
        lvl = lvl+1
        name = name[0:len(name)-len(suffix)]
        r = rm_suffix(name, suffix, lvl)
    elif lvl > 0:
        r = name+'_lvl'+str(lvl)
    else:
        r = name
    return r

# Clean a table or column name removing any glue generated strings to make the names more readable and user friendly.
# if a column name is found in the table_name_list is treated as a foreign key and a suffix _fk is appended.


def clean_name(name_type, tbl_name, col_name, table_name_list, text_to_replace):
    if name_type == 'table':
        tbl_name = rm_suffix(tbl_name, '.val', 0)
        new_name = tbl_name.lower().replace(
            text_to_replace, "").replace(".", "_").replace("_val_", "_")
    elif name_type == 'column':
        col_name = rm_suffix(col_name, '.val', 0)
        new_name = col_name.lower().replace(
            ".val.", "_").replace(".", "_").replace("__", "_")
        if new_name in table_name_list:
            new_name = new_name+"_sk"
        elif new_name == 'id':
            new_name = tbl_name+"_sk"
        elif new_name == 'index':
            new_name = "rownum"
        elif not keep_table_prefix:
            new_name = new_name.replace(tbl_name+"_", "")
    return new_name

# Check if a column is a foreign key (it has a suffix _fk)


def is_foreign_key(col_name, parent_tbl_name):
    name = col_name.replace("_sk", "")
    if not parent_tbl_name == name:
        return col_name[-3:] == "_sk"

# Extract the table name from foreign key column


def get_child_tbl_name(col_name):
    return col_name.replace("_sk", "")

# Check if the relationalized tables need to be denormalized


def is_to_denormalize(num_level_to_denormalize):
    return num_level_to_denormalize > 0

# Determine if the table should be and and which level in the nestes hierarchy is at


def find_tbl_nested_level(dict, tbl):
    for k in dict:
        if tbl in dict[k]:
            lvl = int(k)
            found = 1
            break
        else:
            lvl = 0
            found = 0
    return {'table_nested_level': lvl, 'table_found': found}

# leverages glue native dynamic frame writer to output the final tables to the chosen repository
# added to avoid repeating the code and to dynamically choose the repository to write to depending on the input parameter target_repo


def write_to_targets(tbl_name, dyn_frame, target_path, num_output_files, target_repository):
    dyn_frame = dyn_frame.coalesce(num_output_files)
    if target_repository == 's3' or target_repository == 'all':
        datasink_map[tbl_name] = glueContext.write_dynamic_frame.from_options(frame=dyn_frame, connection_type="s3",
                                                                              connection_options={
                                                                                  "path": target_path},
                                                                              format="glueparquet",
                                                                              transformation_ctx="datasink_map['tbl_name']")

    if target_repository == 'redshift' or target_repository == 'all':
        db_tbl_name = redshift_schema+"."+tbl_name
        glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyn_frame,
                                                       catalog_connection=redshift_connection,
                                                       connection_options={
                                                           "dbtable": db_tbl_name, "database": redshift_db_name},
                                                       redshift_tmp_dir=s3_temp_folder)

    if target_repository == 'none':
        dfc = dyn_frame.toDF().count()
        print('schema for table: ', tbl_name, ' number of rows: ', dfc)
        dfs = dyn_frame.printSchema()
        print(dfs)

# Loop on the list of table's name and on each attributes to standardize all names
# it also standardize the name of the join keys created by the  Relationalize transform
# replacing the default name 'id' with the table name and appending the suffix "_sk"


def prepare_write_table(tbl_name):

    global table_count
    global tables_info_map
    global tables_to_join_map
    global dataframes_map
    global number_nested_levels
    global dynamicframes_map

    table_count = table_count+1

    # set the number_nested_levels for this table
    table_nested_level = tables_info_map[tbl_name]['nested_lvl']

    for col_name in tables_info_map[tbl_name]['columns']:

        # Clean column name and rename the dataframe attribute
        new_col_name = clean_name(
            'column', tbl_name, col_name, tables_info_map.keys(), '')
        dataframes_map[tbl_name] = dataframes_map[tbl_name].withColumnRenamed(
            col_name, new_col_name).drop('rownum')

        # if the column is a foreign key and we have configured the job to run the denormalization we will add the child table to the list of tables to join
        if is_foreign_key(new_col_name, tbl_name):

            # get the child table name and join level
            child_tbl_name = get_child_tbl_name(new_col_name)
            child_tbl_join_lvl = table_nested_level + 1
            number_nested_levels = max(
                number_nested_levels, child_tbl_join_lvl)

            # add the child table to the list of tables to join to for the current teable, and the foreign keys
            tables_info_map[tbl_name]['join_to_tbls'].append(child_tbl_name)
            tables_info_map[tbl_name]['join_col'].append(new_col_name)
            tables_info_map[tbl_name]['has_child'] = True

            tables_info_map[child_tbl_name]['nested_lvl'] = child_tbl_join_lvl

            # add the child table to the next level in the tables_to_join_map dictionary
            if len(tables_to_join_map) == child_tbl_join_lvl:
                tables_to_join_map[str(child_tbl_join_lvl)] = {}
            tables_to_join_map[str(child_tbl_join_lvl)][child_tbl_name] = {
                'join_to_tbls': [], 'join_col': [], 'has_child': False}
            prepare_write_table(child_tbl_name)

    tables_to_join_map[str(tables_info_map[tbl_name]['nested_lvl'])
                       ][tbl_name]['join_to_tbls'] = tables_info_map[tbl_name]['join_to_tbls']
    tables_to_join_map[str(tables_info_map[tbl_name]['nested_lvl'])
                       ][tbl_name]['join_col'] = tables_info_map[tbl_name]['join_col']
    tables_to_join_map[str(tables_info_map[tbl_name]['nested_lvl'])
                       ][tbl_name]['has_child'] = tables_info_map[tbl_name]['has_child']
    # convert to Dynamicframes in preparation for write to repository
    dynamicframes_map[tbl_name] = DynamicFrame.fromDF(
        dataframes_map[tbl_name], glueContext, "dynamicframes_map[tbl_name]")

    # if no denormalization required write the data
    if not is_to_denormalize(num_level_to_denormalize):
        print('writing to ', target_repository)
        write_to_targets(
            tbl_name, dynamicframes_map[tbl_name], s3_target_path_map[tbl_name], num_output_files, target_repository)


def print_tables_by_level():
    print('Number of tables relationalized: ', table_count)
    print('Number of nested levels: ', number_nested_levels)
    for lvl, info in tables_to_join_map.items():
        print('Nesting level: ', lvl)
        for i in info.keys():
            c = dataframes_map[i].count()
            print(i, ' : ', str(c))


def denormalize_table(tables_to_join_map, start_join_level, number_nested_levels, dataframes_map, denormlized_dataframe_map):

    global join_count
    # set the next join level and start looping on all tables to be joined at the current level
    next_join_level = start_join_level+1

    # if the next level is not yet the last level in the nested hierarchy iterate at the next nested level
    if next_join_level < number_nested_levels:
        denormalize_table(tables_to_join_map, next_join_level,
                          number_nested_levels, dataframes_map, denormlized_dataframe_map)

    for left_table in tables_to_join_map[str(start_join_level)]:

        # start the join setting the first dataframe to the one for the left_table
        df_left = dataframes_map[left_table]

        # if the left_table has at least a child table start the loop to join it with all its nested tables
        if tables_to_join_map[str(start_join_level)][left_table]['has_child']:
            for right_table in tables_to_join_map[str(start_join_level)][left_table]['join_to_tbls']:

                # look up the foreign key and set it as join column for left table
                if right_table+"_sk" in tables_to_join_map[str(start_join_level)][left_table]['join_col']:
                    join_col = right_table+"_sk"

                # if the right table had been already denormalized use that dataframe otherwise use the original dataframe
                if right_table in denormlized_dataframe_map:
                    df_right = denormlized_dataframe_map[right_table]
                else:
                    df_right = dataframes_map[right_table]

                # check if there is a naming conflict and resolve it adding the tablename prefix
                left_cols = df_left.schema.names
                right_cols = df_right.schema.names

                for col in right_cols:
                    if col in left_cols and not col == join_col:
                        df_right = df_right.withColumnRenamed(
                            col, right_table+"_"+col)
                        right_cols.append(right_table+"_"+col)
                        right_cols.remove(col)

                # execute the join matching left and right join column names to avoid column duplication in the output dataframe
                df_left = df_left.join(df_right, join_col, how='left_outer')
                df_joined_count = df_left.count()
                join_count = join_count+1
                print('join number: ', join_count)
                print(left_table, ' left join ', right_table,
                      'row count is: ', df_joined_count)

        # add the denormalized data frame to the map and continue with the loop
        denormlized_dataframe_map[left_table] = df_left

    return denormlized_dataframe_map


# native AWS Glue transforms


# read the source table from the glue catalog into a dynamicframe

now = datetime.now()
log_message = "read the source table from the glue catalog into a dynamicframe at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=glue_db_name, table_name=glue_orig_table_name, transformation_ctx="datasource0")

# ResolveChoice.apply is usesd to eliminate ambiguity in case some attributes have different data types in different files or section of a file

now = datetime.now()
log_message = "applying ResolveChoice transform on datasource0 at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

resolvechoice2 = ResolveChoice.apply(
    frame=datasource0, choice="make_struct", transformation_ctx="resolvechoice2")

# DropNullFields.apply is used to drop all the fields that are always null

now = datetime.now()
log_message = "applying DropNullFields transform on resolvechoice2 at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, transformation_ctx="datasource0")

# Relationalize.apply is used to un-nest the JSON files structure creating multiple dynamcic frames
# it automatically introduces keys and foreign keys to allow to join the dynamic frames back together (creating a fully relational schema)
# the dynamic frames generated are grouped in a dynamic frame collection

now = datetime.now()
log_message = "un-nest the JSON files structure applying Relationalize transform on dropnullfields3 at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

relationalize4 = Relationalize.apply(
    frame=dropnullfields3, staging_path=s3_temp_folder, name=root_table, transformation_ctx="relationalize4")

# at this point the data is completely un-nested;
# it could be possible to write the result in the target systems without any additional processing.
# the following part of the code will cleanse the names of tables and columns to make it more user friendly

# Loop on the list of dynamic frames name and cleanse it if needed

now = datetime.now()
log_message = "running loop to cleanse table names at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

text_to_replace = root_table+"_"

for df_name in relationalize4.keys():

    if (df_name == root_table):
        tbl_name = root_table
        if len(relationalize4.select(root_table).toDF().schema.names) == 1:
            tbl_name = relationalize4.select(root_table).toDF().schema.names[0]
            df_name = add_prefix(tbl_name, root_table)
            root_table = tbl_name

        tbl_join_lvl = 0
        has_child = True
        tables_to_join_map = {
            '0': {tbl_name: {'join_to_tbls': [], 'join_col': []}}}
    else:
        tbl_name = clean_name('table', df_name, '', [], text_to_replace)
        tbl_join_lvl = None
        has_child = False

    # create a list of S3 output path indexed by table name
    s3_target_path_map[tbl_name] = s3_target_path+tbl_name
    # create a list of dataframes indexed by table name
    dataframes_map[tbl_name] = relationalize4.select(df_name).toDF()
    # create a list of columns indexed by table name
    if tbl_name not in tables_info_map:
        tables_info_map[tbl_name] = {'columns': dataframes_map[tbl_name].schema.names,
                                     'has_child': has_child, 'nested_lvl': tbl_join_lvl, 'join_to_tbls': [], 'join_col': []}

# Loop on the list of table's name and on each attributes to standardize all names
# it also standardize the name of the join keys created by the  Relationalize transform
# replacing the default name 'id' with the table name and appending the suffix "_sk"

now = datetime.now()
log_message = "cleansing the columns names and writing relationalized tables at " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

prepare_write_table(root_table)

# Print out the tables that have been relationalized for each nested level

now = datetime.now()
log_message = " Print out the tables that have been relationalized for each nested level " + \
    now.strftime("%Y-%m-%d %H:%M:%S")
logger.info(log_message)
print(log_message)

print_tables_by_level()

# Optional: denormalize tables based on the parameter num_level_to_denormalize
# !! if the json file to denormalize there might be too many joins and processing might be slow
# !! also the final table might become very wide and difficult to use for end users
# check first how many tables are generated and how they are joined
# only then select the right num_level_to_denormalize based on the use case and data model and perform the denormalization

# if no denormalization needed we are done

if not is_to_denormalize(num_level_to_denormalize):
    print('Job completed Successfully')
else:
    now = datetime.now()
    log_message = "denormalizing tables at " + \
        now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(log_message)
    print(log_message)

    # select the nested level wheere to start the denormalization based on input parameter number_nested_levels
    if num_level_to_denormalize > number_nested_levels:
        start_join_level = 0
    else:
        start_join_level = (number_nested_levels - num_level_to_denormalize)
        print('start denormalizing table(s)')

    # start denormalization
    denormlized_dataframe_map = denormalize_table(
        tables_to_join_map, start_join_level, number_nested_levels, dataframes_map, denormlized_dataframe_map)

    now = datetime.now()
    log_message = "writing denormalized tables at " + \
        now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(log_message)
    print(log_message)

    # loop on the dynamic frames map to select the right dynamic frame to write, we need to write the denormalized dynamic frames and any dynamic frames that have not been denormalized
    for tbl in dynamicframes_map:
        # find the table nested level number and if the table has been denormalized or not
        table_with_nested_level = find_tbl_nested_level(
            tables_to_join_map, tbl)

        # select only the tables at a nested level equal or less to start_join_level
        if table_with_nested_level['table_nested_level'] <= start_join_level:
            # Need to replace the dynamic frame with the denormalized on ONLY for the parent tables, this is true only for tables that satisfy both the
            # following condition the table has childrens (table_with_nested_level['table_found'] == 1) and it is at the correct level in the nested level hierarchies ( table_with_nested_level['table_nested_level'] == start_join_level )
            if table_with_nested_level['table_found'] == 1 and table_with_nested_level['table_nested_level'] == start_join_level:
                dynamicframes_map[tbl] = DynamicFrame.fromDF(
                    denormlized_dataframe_map[tbl], glueContext, "dynamicframes_map[tbl]")
            write_to_targets(
                tbl, dynamicframes_map[tbl], s3_target_path_map[tbl], num_output_files, target_repository)


job.commit()
