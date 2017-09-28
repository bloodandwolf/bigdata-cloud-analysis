# encoding=utf-8

from collections import Counter
import xlsxwriter


def write_and_plot_one_item(workbook, topFailCause, currentItem, totalNum, plot_type):
    # print('开始写入Pie的Top数据...')
    worksheet = workbook.add_worksheet(currentItem)
    bold = workbook.add_format({'bold': 1})
    border = workbook.add_format({'border': 1})
    headings = [currentItem, '次数', '占比']
    worksheet.write_row('A1', headings, bold)

    for i in range(len(topFailCause)):
        worksheet.write(i + 1, 0, str(topFailCause[i][0]), border)
        worksheet.write(i + 1, 1, topFailCause[i][1], border)
        worksheet.write(i + 1, 2, str(round(1.0 * topFailCause[i][1] / totalNum * 100, 4)) + '%', border)

    if (plot_type == 'bar'):
        plot_one_bar(workbook, currentItem, '次数', 'A1', 'A2', 'A' + str(len(topFailCause) + 1), 'B2',
                     'B' + str(len(topFailCause) + 1), 'D2')
    elif (plot_type == 'pie'):
        plot_one_pie(workbook, currentItem, 'A1', 'A2', 'A' + str(len(topFailCause) + 1), 'B2',
                     'B' + str(len(topFailCause) + 1), 'D2')
    elif (plot_type == 'line'):
        pass
    else:
        pass


def plot_one_bar(workbook, sheetName, yLable, whereIsName,
                 whereIsCategories_start, whereIsCategories_end,
                 whereIsValues_start, whereIsValues_end,
                 whereToPlot):
    name = '=' + sheetName + '!$' + whereIsName[0] + '$' + whereIsName[1:]
    categories = '=' + sheetName + '!$' + whereIsCategories_start[0] + '$' + whereIsCategories_start[1:] + ':$' + \
                 whereIsCategories_end[0] + '$' + whereIsCategories_end[1:]
    values = '=' + sheetName + '!$' + whereIsValues_start[0] + '$' + whereIsValues_start[1:] + ':$' + whereIsValues_end[
        0] + '$' + whereIsValues_end[1:]

    chart1 = workbook.add_chart({'type': 'column'})
    chart1.add_series({
        'name': name,
        'categories': categories,
        'values': values,
        'data_labels': {'value': 1},
        'marker': {'type': 'automatic'},
    })
    xLable = sheetName
    yLable = yLable
    chart1.set_title({'name': sheetName + 'vs' + yLable})
    chart1.set_x_axis({'name': xLable})
    chart1.set_y_axis({'name': yLable})
    worksheet = workbook.get_worksheet_by_name(sheetName)
    worksheet.insert_chart(whereToPlot, chart1, {'x_offset': 25, 'y_offset': 10})


def plot_one_pie(workbook, sheetName, whereIsName,
                 whereIsCategories_start, whereIsCategories_end,
                 whereIsValues_start, whereIsValues_end,
                 whereToPlot):
    name = '=' + sheetName + '!$' + whereIsName[0] + '$' + whereIsName[1:]
    categories = '=' + sheetName + '!$' + whereIsCategories_start[0] + '$' + whereIsCategories_start[1:] + ':$' + \
                 whereIsCategories_end[0] + '$' + whereIsCategories_end[1:]
    values = '=' + sheetName + '!$' + whereIsValues_start[0] + '$' + whereIsValues_start[1:] + ':$' + whereIsValues_end[
        0] + '$' + whereIsValues_end[1:]

    chart1 = workbook.add_chart({'type': 'pie'})
    chart1.add_series({
        'name': name,
        'categories': categories,
        'values': values,
        'data_labels': {'value': 1},
        'marker': {'type': 'automatic'},
    })
    chart1.set_title({'name': sheetName})
    worksheet = workbook.get_worksheet_by_name(sheetName)
    worksheet.insert_chart(whereToPlot, chart1, {'x_offset': 25, 'y_offset': 10})


def plot_one_line(workbook, sheetName, whereIsName,
                  whereIsCategories_start, whereIsCategories_end,
                  whereIsValues_start, whereIsValues_end,
                  whereToPlot):
    name = '=' + sheetName + '!$' + whereIsName[0] + '$' + whereIsName[1:]
    categories = '=' + sheetName + '!$' + whereIsCategories_start[0] + '$' + whereIsCategories_start[1:] + ':$' + \
                 whereIsCategories_end[0] + '$' + whereIsCategories_end[1:]
    values = '=' + sheetName + '!$' + whereIsValues_start[0] + '$' + whereIsValues_start[1:] + ':$' + whereIsValues_end[
        0] + '$' + whereIsValues_end[1:]

    chart1 = workbook.add_chart({'type': 'line'})
    chart1.add_series({
        'name': name,
        'categories': categories,
        'values': values,
        'data_labels': {'value': 1},
        'marker': {'type': 'automatic'},
    })
    chart1.set_x_axis({'name': name})
    chart1.set_y_axis({'name': '出现问题次数(次数/千台)'})
    chart1.set_title({'name': sheetName})
    worksheet = workbook.get_worksheet_by_name(sheetName)
    worksheet.insert_chart(whereToPlot, chart1, {'x_offset': 25, 'y_offset': 10})


def write_every_item_sheet(workbook, callFailData, heading, currentItem, rowLength):
    item_top = 20
    failCauseData = callFailData[currentItem]
    failCauseCounts = Counter(failCauseData)
    topItems = failCauseCounts.most_common(item_top)

    print('当前的要处理的项为==' + currentItem)
    plot_type = 'bar'
    write_and_plot_one_item(workbook, topItems, currentItem, rowLength, plot_type)
    # print('当前要处理的项，Top处理结束')

    headings = []
    for i in heading:
        headings.append((i, '次数', '全局占比', '局部占比'), )

    # 对topItems中的前五个进行详细分析
    topItems_five = 20
    if (len(topItems) < 20):
        topItems_five = len(topItems)
    else:
        topItems_five = 20

    row = 25 + topItems_five

    for topItems_five_topi in range(0, topItems_five):
        worksheet = workbook.get_worksheet_by_name(currentItem)
        bold = workbook.add_format({'bold': 1})
        border = workbook.add_format({'border': 1})
        worksheet.write_row(row, 0, [currentItem, '次数', '占比'], bold)
        worksheet.write(row + 1, 0, str(topItems[topItems_five_topi][0]), border)
        worksheet.write(row + 1, 1, topItems[topItems_five_topi][1], border)
        worksheet.write(row + 1, 2, str(round(1.0 * topItems[topItems_five_topi][1] / rowLength * 100, 4)) + '%',
                        border)

        topItems_five_top5 = 10
        row = row + 3
        for item in headings:
            headings1 = list(item)
            write_every_top(workbook, callFailData, headings1, currentItem, row, topItems_five_top5, topItems_five_topi,
                            rowLength)
            row = row + 15
        row = row + 2


# 对每一项中的前
def write_every_top(workbook, callFailData, headings, currentItem, row, top, topi, rowLength):
    failCauseData = callFailData[currentItem]
    failCauseCounts = Counter(failCauseData)
    topFailCause = failCauseCounts.most_common(20)

    firstFailCause = topFailCause[topi][0]
    firstFailCauseDF = callFailData.loc[callFailData[currentItem] == firstFailCause]
    firstFailCauseStartLacDF = firstFailCauseDF[headings[0]]
    firstFailCauseStartLacDFTop = Counter(firstFailCauseStartLacDF).most_common(top)

    worksheet = workbook.get_worksheet_by_name(currentItem)
    bold = workbook.add_format({'bold': 1})
    border = workbook.add_format({'border': 1})

    worksheet.write(row, 3, headings[0], bold)
    worksheet.write(row, 4, headings[1], bold)
    worksheet.write(row, 5, headings[2], bold)
    worksheet.write(row, 6, headings[3], bold)
    row += 1
    length = 0
    for i in range(len(firstFailCauseStartLacDFTop)):
        length += firstFailCauseStartLacDFTop[i][1]

    for i in range(len(firstFailCauseStartLacDFTop)):
        worksheet.write(i + row, 3, str(firstFailCauseStartLacDFTop[i][0]), border)
        worksheet.write(i + row, 4, firstFailCauseStartLacDFTop[i][1], border)
        worksheet.write(i + row, 5, str(round(1.0 * firstFailCauseStartLacDFTop[i][1] / rowLength * 100, 4)) + '%',
                        border)
        worksheet.write(i + row, 6, str(round(1.0 * firstFailCauseStartLacDFTop[i][1] / length * 100, 4)) + '%', border)


def write_data_into_excel_overall(data, file_name):
    workbook = xlsxwriter.Workbook(file_name)
    bold = workbook.add_format({'bold': 1})
    border = workbook.add_format({'border': 1})

    rowLength = data.shape[0]
    for item in data.columns:
        print('正在导出 ' + item)
        worksheet = workbook.add_worksheet(item)
        worksheet.write(0, 0, 'Top', bold)
        worksheet.write(0, 1, str(item), bold)
        worksheet.write(0, 2, '次数', bold)
        worksheet.write(0, 3, '占比', bold)

        counter_every_item = data[item].value_counts()
        length_of_counter = len(counter_every_item)
        for row in range(length_of_counter):
            worksheet.write(row + 1, 0, row + 1, border)
            worksheet.write(row + 1, 1, str(counter_every_item.index[row]), border)
            worksheet.write(row + 1, 2, counter_every_item.values[row], border)
            worksheet.write(row + 1, 3, str(round(1.0 * counter_every_item.values[row] / rowLength * 100, 4)) + '%',
                            border)
        length = length_of_counter
        if (length_of_counter > 100):
            length = 100
        plot_one_pie(workbook, item, 'B1', 'B2', 'B' + str(length_of_counter + 1), 'C2', 'C' + str(length_of_counter + 1), 'E2')
    workbook.close()


def write_data_into_excel_every_item(data, file_name):
    workbook = xlsxwriter.Workbook(file_name)

    rowLength = data.shape[0]

    columns_raw = list(data.columns)
    columns_copy = list(data.columns)
    for currentItem in data.columns:
        columns_copy.remove(currentItem)
        write_every_item_sheet(workbook, data, columns_copy, currentItem, rowLength)
        columns_copy = list(columns_raw)

