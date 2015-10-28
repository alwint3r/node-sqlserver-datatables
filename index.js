'use strict';

const async = require('async');
const mssql = require('mssql');

class MSSQLDatatable {
    constructor(link) {
        this.link = link;
    }

    get(tableName, options, callback) {
        const columns = getColumns(options);
        const response = {
            draw: 0,
            recordsTotal: 0,
            recordsFiltered: 0,
            data: [],
            error: null,
        };

        async.parallel({
            getRowsLength(_callback) {
                const query = `SELECT COUNT(*) AS RowCounts
                    FROM ${tableName}
                    ${buildSearch(options)}`;

                const request = new mssql.Request(this.link);

                request.query(query, (err, recordset) => {
                    if (err) {
                        return _callback(err);
                    }

                    response.recordsTotal = recordset[0].RowCounts;
                    response.recordsFiltered = response.recordsTotal;

                    return _callback(null);
                });
            },

            getData(_callback) {
                const query = `SELECT ${columns} FROM ${tableName}
                    ${buildSearch(options)}
                    ${getOrder(options)}
                    ${paginateQuery(options)}`;
                const request = new mssql.Request(this.link);

                request.query(query, (err, recordset) => {
                    if (err) {
                        return _callback(err, response);
                    }

                    response.data = recordset;

                    return _callback(null);
                });
            },
        }, err => callback(err, response));
    }
};

module.exports = MSSQLDatatable;

function getColumns(options, array) {
    const columns = _.pluck(options.columns, 'data');

    if (array) {
        return columns;
    }

    return columns.join(',');
}

function getSearchableColumns(options) {
    const columns = _.filter(options.columns, column => column.searchable === 'true');
    return columns;
}

function buildSearch(options) {
    const searchableColumns = getSearchableColumns(options);
    let searchString = 'WHERE ';

    if (!options.search.value) {
        return '';
    }

    _.forEach(searchableColumns, column => {
        searchString += `${column.data} LIKE '%${options.search.value}%' OR `;
    });

    searchString = searchString.slice(0, searchString.length - 4);
    return searchString;
}

function getOrder(options) {
    const columns = getColumns(options, true);
    const orderField = columns[parseInt(options.order[0].column)];
    const orderDirection = options.order[0].dir.toUpperCase();

    return `ORDER BY ${orderField} ${orderDirection}`;
}

function paginateQuery(options) {
    if (options.length < 0) {
        return '';
    }

    return `OFFSET ${options.start} ROWS FETCH NEXT ${options.length} ROWS ONLY`;
}
