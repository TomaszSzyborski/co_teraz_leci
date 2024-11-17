    $(document).ready(function() {
        $('#channelTable').DataTable({
            "paging": true,
            "ordering": true,
            "language": {
                "sProcessing":    "Przetwarzam...",
                "sZeroRecords":   "Nie znaleziono wyników",
                "sEmptyTable":    "Pusta tabela",
                "sInfo":          "Od _START_ do _END_ z całości wyników _TOTAL_",
                "sInfoEmpty":     "Od 0 do 0 z 0 wyników",
                "sInfoFiltered":  "(wyfiltrowano _MAX_ wynikow)",
                "sInfoPostFix":   "",
                "sSearch":        "Wyszukaj:",
                "sUrl":           "",
                "sLengthMenu":   "Pokaż _MENU_ wyników",
                "sInfoThousands":  ",",
                "sLoadingRecords": "Ładuję...",
                "oPaginate": {
                    "sFirst":    "Pierwsza",
                    "sLast":     "Ostatnia",
                    "sNext":     "Następna strona",
                    "sPrevious": "Poprzednia strona"
                },
                "oAria": {
                    "sSortAscending":  ": Posortuj rosnąco",
                    "sSortDescending": ": Posortuj malejąco"
                }
            }
        });
    });

