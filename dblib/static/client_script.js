//making life a bit happier
function handleTabKey(event) {
  const tabKeyCode = 9;
  const parenthesesKeyCode = 219;
  const enter = 13;
  const textarea = document.getElementById("large_text_field");
  const currTxt = textarea.value;
  const cursorPosition = textarea.selectionStart;
  switch (event.keyCode) {
    case tabKeyCode:
      event.preventDefault();
      event.stopPropagation();
      textarea.value = currTxt.substring(0, cursorPosition) + '\t' + currTxt.substring(cursorPosition);
      textarea.selectionStart = cursorPosition + '\t'.length;
      textarea.selectionEnd = textarea.selectionStart;
      break;
    case parenthesesKeyCode:
      break;
      event.preventDefault();
      event.stopPropagation();
      textarea.value = currTxt.substring(0, cursorPosition) + '()' + currTxt.substring(cursorPosition);
      textarea.selectionStart = cursorPosition + 1;
      textarea.selectionEnd = textarea.selectionStart;
      break;
    case enter:
      if (currTxt[cursorPosition - 1] === '(' && currTxt[cursorPosition] === ')') {
        event.preventDefault();
        event.stopPropagation();
        textarea.value = currTxt.substring(0, cursorPosition) + '\n\t\n);\n' + currTxt.substring(cursorPosition + 1);
        textarea.selectionStart = cursorPosition + 2;
        textarea.selectionEnd = textarea.selectionStart;
      }
      else if (checkForTabinRow(currTxt, cursorPosition)) {
        event.preventDefault();
        event.stopPropagation();
        textarea.value = currTxt.substring(0, cursorPosition) + '\n\t\n' + currTxt.substring(cursorPosition + 1);
        textarea.selectionStart = cursorPosition + 2;
        textarea.selectionEnd = textarea.selectionStart;
      }
  }
}

function checkForTabinRow(text, index) {
  for (let i = index - 1; i >= 0; i--) {
    if (text[i] === '\n') {
      return false;
    }
    else if (text[i] === '\t') {
      return true;
    }
  }
}

async function sendSomething(event) {
  event.preventDefault(); // Prevent the form from submitting normally
  console.log('please2')
  const text = document.getElementById('large_text_field').value;
  const res = await fetch('/api/database/commands', {
      method: 'POST',
      headers: {
          'Content-Type': 'application/json'
      },
      body: JSON.stringify({ "text": text })
  });
  const data = await res.text();
  document.getElementById('large_text_field').value = '';
  console.log(data);
  document.getElementById('large_message_field').value += data + '\n' ;
  pleaseForTheLoveOfGod();
  pleaseForTheLoveOfGod2();
  // const myjson = await res.json();
  // console.log(myjson);
}

//--------------------DB------------------------

async function pleaseForTheLoveOfGod() {
  console.log('please1')
  const listContent = await fetch('/api/database/db_list', {
      method: 'GET',
      headers: {
          'Content-Type' : 'application/json'
      }
  });
  console.log(listContent);
  populateList(await listContent.json());
}

function populateList(databases) {
  const dbList = document.getElementById('dbList');
  dbList.innerHTML = '';
  databases.forEach(database => {
    const dbButton = document.createElement('button');
    dbButton.textContent = database;
    dbButton.textContent = database;
    dbButton.dataset.value = database;
    dbButton.addEventListener('click', () => {
      selectCurrDB(dbButton.textContent);
    });
    dbButton.classList.add('button');
    /*NAJO OMGOMGOMGOMGOGMMOMGMOMGGMGOMGG ---- MAJD KI KELL VEGYEM KULON FUGGVENYBE*/
    const contextMenu = document.createElement('div');
    contextMenu.classList.add('context-menu');
    const deleteButton = document.createElement('button');
    deleteButton.textContent = 'Drop Database';
    deleteButton.classList.add('context-menu-button');
    deleteButton.dataset.value = database;
    deleteButton.addEventListener('click', () => {
      deleteDatabase(deleteButton.dataset.value);
      contextMenu.style.display = 'none';
    });
    contextMenu.appendChild(deleteButton);

    const createTable = document.createElement('button');
    createTable.textContent = 'Create Table';
    createTable.classList.add('context-menu-button');
    createTable.dataset.value = database;
    createTable.addEventListener('click', () => {
      createTableDialog();
      contextMenu.style.display = 'none';
    });
    contextMenu.appendChild(createTable);

    document.body.appendChild(contextMenu);
    dbButton.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      contextMenu.style.left = e.clientX + 'px';
      contextMenu.style.top = e.clientY + 'px';
      contextMenu.style.display = 'block';
      contextMenu.style.flexDirection = 'row';
      document.addEventListener('click', hideContextMenu);
    });
    dbList.appendChild(dbButton);
});
}

function hideContextMenu(e) {
  const contextMenu = document.querySelector('.context-menu');
  if (!contextMenu.contains(e.target)) {
    contextMenu.style.display = 'none';
    document.removeEventListener('click', hideContextMenu);
  }
}

async function selectCurrDB(name) {
  console.log('please4')
  const res = await fetch('/api/database/select_db', {
      method: 'POST',
      headers: {
          'Content-Type': 'application/json'
      },
      body: JSON.stringify({ "curr_db": name })
  });
  deselectAllButtons();
  document.querySelector(`button[data-value="${name}"].button`).classList.add('active');
  pleaseForTheLoveOfGod2()
}

function deselectAllButtons() {
  const buttons = document.getElementsByClassName('button');
  for (const button of buttons) {
    button.classList.remove('active');
  }
}


//--------------------TABLES---------------------

async function pleaseForTheLoveOfGod2() {
  console.log('please3')
  const listContent = await fetch('/api/table/table_list', {
      method: 'GET',
      headers: {
          'Content-Type' : 'application/json'
      }
  });
  populateTableList(await listContent.json());
}

function populateTableList(tables) {
  const tableList = document.getElementById('tableList');
  tableList.innerHTML = ' ';
  tables.forEach(table => {
    const li = document.createElement('li');
    li.textContent = table;
    tableList.appendChild(li);
  })
}

//--------------------DIALOG---------------------
//--------------------CR DB DIALOG---------------------
function createDatabaseDialog() {
  const dialog = document.getElementById("createDatabaseDialog");
  dialog.style.display = "block";
}

function createDatabaseDialogOk() {
  const newDatabaseNameInput = document.getElementById("newDatabaseNameInput");
  if (newDatabaseNameInput.value) {
    createDatabase(newDatabaseNameInput.value); //funnction that creates the db
    // Close the dialog
    const dialog = document.getElementById("createDatabaseDialog");
    dialog.style.display = "none";
  }
}

function createTableDialogCancel() {
  const newDatabaseNameInput = document.getElementById("newDatabaseNameInput");
  newDatabaseNameInput.value = "";
  // Close the dialog
  const dialog = document.getElementById("createDatabaseDialog");
  dialog.style.display = "none";
}

//--------------------CR TABLE DIALOG---------------------
function createTableDialog() {
  const dialog = document.getElementById("createTableDialog");
  dialog.style.display = "block";
}

function addColumn() {
  const collInp = document.getElementById("columnNameInput").value;
  const type = document.getElementById("columnTypeSelect").value;
  const collList = document.getElementById("coll-list");
  const li = document.createElement('li');
  li.textContent = collInp;
  const typeSpan = document.createElement('span');
  typeSpan.textContent = `Type: ${type}`;
  typeSpan.style.marginLeft = '10px';
  li.appendChild(typeSpan);
  const delButton = document.createElement('button');
  delButton.textContent = '-';
  delButton.onclick = () => {
    li.remove();
  }
  li.appendChild(delButton);
  collList.appendChild(li);
}
/* KI KELL JAVITSAM*/
function checkCollList() {
  const collInp = document.getElementById("columnNameInput").value.trim();
  const colls = document.getElementById("coll-list");
  const existingColl = Array.from(colls.getElementsByTagName("li")).find(li => li.textContent.trim() === collInp)
  if (!existingColl) {
    addColumn();
    return true;
  }
  return false;
}

function createTableDialogOk() {
    const newDatabaseNameInput = document.getElementById("newTableInput");
    if (newDatabaseNameInput.value) {
        createTableFromDialog(newDatabaseNameInput.value); //funnction that creates the db
        // Close the dialog
        const dialog = document.getElementById("createTableDialog");
        dialog.style.display = "none";
    }
}

function createTableDialogCancel() {
    const newDatabaseNameInput = document.getElementById("newTableInput");
    newDatabaseNameInput.value = "";
    const columnNameInput = document.getElementById("columnNameInput");
    columnNameInput.value = "";
    const columnTypeSelect = document.getElementById("columnTypeSelect");
    columnTypeSelect.value = "text";
    columns = [];
    // Close the dialog
    const dialog = document.getElementById("createTableDialog");
    dialog.style.display = "none";
}


async function createDatabase(databaseName) {
  const commandString = 'CREATE DATABASE ' + databaseName;
  const res = await fetch('/api/database/commands', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify({ "text": commandString })
  });
  newDatabaseNameInput.value = "";
  pleaseForTheLoveOfGod();
}

async function createTable(databaseName) {
  const commandString = 'CREATE TABLE ' + databaseName + ' (\n  valami INT\n);' ;
  const res = await fetch('/api/database/commands', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify({ "text": commandString })
  });
  newDatabaseNameInput.value = "";
  pleaseForTheLoveOfGod();
}

async function deleteDatabase(databaseName) {
  const commandString = 'DROP DATABASE ' + databaseName;
  const res = await fetch('/api/database/commands', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify({ "text": commandString })
  });
  pleaseForTheLoveOfGod();
}

async function createTableFromDialog(tableName) {
  const columnList = document.getElementById("column-list");
  const columnElements = Array.from(columnList.getElementsByTagName("li"));
  const columnDefs = columnElements.map((column, index) => {
    const columnName = column.textContent;
    const columnType = column.getElementsByTagName("span")[0].textContent.split(": ")[1];
    return `${columnName} ${columnType}${index < columnElements.length - 1 ? ", " : ")"}`;
  }); 
  const commandString = `CREATE TABLE ${tableName} (${columnDefs.join(" ")})`;
  console.log(commandString);
  return true;
  const res = await fetch('/api/database/commands', { 
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify({ "text": commandString })
  });
}

pleaseForTheLoveOfGod()
