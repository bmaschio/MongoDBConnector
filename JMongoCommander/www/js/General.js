function popolatePresenzeTable() {
    var data = {
        date: '5/09/2016',
        repartoName: $("#repartoSelect").val()
    }
    $.ajax({
        url: '/getPresenzeTable', <!--nome della operazione esposta da leonardo  -->
        dataType: 'json', <!-- tipo dato -->
        type: 'POST', <!-- tipo chiamata -->
        contentType: 'application/json;charset=UTF-8', <!-- Tipo di ritorno-->
        success: function(response) { <!-- Funzione se va tutto bene -->
        }
    })
}
$(document).ready(function() {
$('#jstree').jstree({
"core": {
"check_callback": true,
"data": [{
    "id": "dbMaster",
    "parent": "#",
    "text": "DataBase"
}]
}
})

$('#jstree').on('ready.jstree', function(e, data) {
$.ajax({
url: '/getDatabases', <!--nome della operazione esposta da leonardo  -->
dataType: 'json', <!-- tipo dato -->
type: 'POST', <!-- tipo chiamata -->
contentType: 'application/json;charset=UTF-8', <!-- Tipo di ritorno-->
success: function(response) {
    if (typeof response.db == 'object' && response.db instanceof Array) {
        for (i = 0; i < response.db.length; i++) {
            $('#jstree').jstree('create_node', $('#dbMaster'), {
                "text": response.db,
                "id": response.db
            }, "last", false, false);
        }
    }
}
})

})

})
})

})

})
