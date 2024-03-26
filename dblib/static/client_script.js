//making life a bit happier
function handleTabKey(event) {
  const tabKeyCode = 9;
  const parenthesesKeyCode = 57;
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
  document.getElementById('large_message_field').value += data + '\n';
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
      'Content-Type': 'application/json'
    }
  });
  console.log(listContent);
  populateList(await listContent.json());
}

function populateList(databases) {
  const dbList = document.getElementById('dbList');
  dbList.innerHTML = '';
  databases.forEach(database => {
    const button = document.createElement('button');
    button.textContent = database;
    button.textContent = database;
    button.dataset.value = database;
    button.addEventListener('click', () => {
      selectCurrDB(button.textContent);
    });
    button.classList.add('button');
    dbList.appendChild(button);
  });
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
      'Content-Type': 'application/json'
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

function createDatabaseDialogCancel() {
  const newDatabaseNameInput = document.getElementById("newDatabaseNameInput");
  newDatabaseNameInput.value = "";
  // Close the dialog
  const dialog = document.getElementById("createDatabaseDialog");
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

pleaseForTheLoveOfGod()