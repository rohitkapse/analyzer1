import os
import logging
import re

logging.basicConfig(filename="Analyser.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger(__file__)


def get_files_in_directory():
    """
    input: None
    :return: a list with all the files in the directory with it's full path
    """
    return [os.path.join(os.getcwd(), file) for file in os.listdir(os.getcwd())]


def process_file(filename):
    """
    :param filename:
    :return:Extracts data and return a list of records to be written to the final file
    """
    file_name = filename.split("\\")[-1].lstrip().rstrip()
    folder_name = os.getcwd()

    try:
        with open(filename) as file:
            records_temp = list(map(lambda r: r[6:72].upper(), file.readlines()))
            records = list(filter(lambda r: len(r.replace(" ", "")) > 0, records_temp))
            process_sql = False
            process_cursor = False
            capture_tables = False
            cursor_name = "N/A"
            cursor_declaration = []
            table_name = []
            extracted_data = []
            maybe_cursor = False
            cursor_line = 0

            for record in records:
                try:
                    # skip comments
                    if record[0] == "*":
                        continue

                    #see the second line for cursor
                    if maybe_cursor and process_cursor:
                        cursor_line += 1


                    # set the file name as program name if it is cobol
                    if "PROGRAM-ID" in record:
                        file_name = record.split("ID.")[1].split(".")[0].lstrip().rstrip()

                    # set sql stop flag
                    if process_sql and "END-EXEC" in record and process_cursor:
                        for table in table_name:
                            if table.lstrip().rstrip() not in ["INNER JOIN", "OUTER JOIN", "LEFT OUTER JOIN",
                                                               "LEFT INNER JOIN", "JOIN",
                                                               "FOR READ ONLY WITH UR"]:
                                extracted_data.extend([file_name + "~" + cursor_name + "~" + table
                                                       + "~" + "[" + " ".join(
                                    cursor_declaration) + "]" + "~" + folder_name + "\n"])
                        process_sql = False
                        process_cursor = False
                        cursor_declaration.clear()
                        table_name.clear()

                    # set start of sql
                    if "EXEC SQL" in record:
                        process_sql = True

                    if process_sql:

                        if "DECLARE " in record and " CURSOR " in record:
                            process_cursor = True
                            cursor_name = record.split(" CURSOR ")[0].split()[-1]

                        if "DECLARE " in record and " CURSOR " not in record:
                            cursor_name = record.split()[1]
                            maybe_cursor = True
                            process_cursor = True

                        if maybe_cursor and process_cursor and cursor_line == 1:
                            if "CURSOR " in record:
                                maybe_cursor = False
                                cursor_line = 0
                            else:
                                maybe_cursor = False
                                process_cursor = False
                                cursor_declaration.clear()
                                cursor_line = 0

                        # set process cursor flag
                        if process_cursor:
                            cursor_declaration.extend([record.replace("  ", "")])

                        if capture_tables:
                            list_of_words = [" WHERE ", " GROUP BY ", " ORDER BY "]
                            words_to_search = re.compile("|".join(list_of_words))

                            where_index = 0
                            group_index = 0
                            order_index = 0

                            if words_to_search.search(record):
                                capture_tables = False
                                if "WHERE" in record:
                                    where_index = record.replace(" ", "").index("WHERE")
                                if "GROUP BY" in record:
                                    group_index = record.replace(" ", "").index("GROUP BY")
                                if "ORDER BY" in record:
                                    order_index = record.replace(" ", "").index("ORDER BY")

                                index_to_check = [i for i in [where_index, group_index, order_index] if i > 0]
                                if index_to_check:
                                    index_end = min(index_to_check)

                                temp_tbl_names = list(filter(lambda x: len(x) > 1, record[:index_end + 1].split()))
                                table_name.extend(temp_tbl_names)
                                temp_tbl_names.clear()
                            else:
                                temp_tbl_names = list(filter(lambda x: len(x) > 1, record.split()))
                                table_name.extend(list(map(lambda x: x.split()[0].replace(",", ""), temp_tbl_names)))
                                temp_tbl_names.clear()

                        if " FROM " in record and process_cursor:
                            capture_tables = True
                            temp_tbl_names = list(filter(lambda x: len(x) > 1, record.split(" FROM ")[1].split()))
                            table_name.extend(list(map(lambda x: x.split()[0].replace(",", ""), temp_tbl_names)))
                            temp_tbl_names.clear()


                except Exception as e:
                    logger.error("exception " + str(e) + " occured " + "record: " + record)
                    continue

            else:
                return extracted_data

    except Exception as e:
        logger.error("exception " + str(e) + " occured")


def write_report(report_data):
    """
    Create the final excel report
    :param report_data:
    :return: None

    """
    with open("analysis_report.txt", "w") as final_report:
        final_report.writelines(report_data)


def main():
    """
    Main processing
    creates the excel file with required details
    """
    # records to be written to the final file
    records_to_write = ["program / file name ~ Cursor Name ~  Tables ~ Declaration ~ Source_Path" + "\n"]

    # get a list of files in the directory
    files_list = get_files_in_directory()

    # loop through files and call process file
    for file in files_list:
        report_data = process_file(file)
        if report_data:
            records_to_write.extend(report_data)
    else:
        write_report(records_to_write)


if __name__ == "__main__":
    main()




