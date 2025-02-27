export const validDocumentId = (id: any): string => {
    console.log("Received ID:", id);

    if (typeof id !== "string" || !id.trim()) {
        return "";
    }


    const customHexMapping = {
        'g': 'a', 'h': 'b', 'i': 'c', 'j': 'd', 'k': 'e', 'l': 'f',
        'm': '1', 'n': '2', 'o': '3', 'p': '4', 'q': '5', 'r': '6',
        's': '7', 't': '8', 'u': '9', 'v': 'a', 'w': 'b', 'x': 'c',
        'y': 'd', 'z': 'e'
    };


    const cleanedId = id.toLowerCase().replace(/[^0-9a-f]/g, (char) => {
        return customHexMapping[char] || '';
    });


    if (cleanedId.length === 24) {
        return cleanedId;
    } else if (cleanedId.length > 24) {
        return cleanedId.slice(0, 24);
    } else {
        return cleanedId.padEnd(24, "0");
    }
};


