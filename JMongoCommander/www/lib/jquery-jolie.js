// Global Jolie object
var Jolie = {
	// Calls an operation at the originating server using JSON
	call: function( operation, data, callback, callbackError ) {
		$.ajax({
			url: '/' + operation,
			dataType: 'json',
			type: 'POST',
			contentType: 'application/json;charset=UTF-8',
			success: callback,
			error: callbackError,
			data: JSON.stringify( data )
		});
	},
	
	// Calls an operation at the specified
	// service published by the originating server using JSON
	callService: function( service, operation, data, callback ) {
		$.ajax({
			url: '/!/' + service + '!/' + operation,
			dataType: 'json',
			type: 'POST',
			contentType: 'application/json',
			success: callback,
			data: JSON.stringify( data )
		});
	},
	widgets: {}
};

// Make jHome global
window.Jolie = Jolie;
