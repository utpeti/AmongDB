//making life a bit happier
function handleTabKey(event) {
  const tabKeyCode = 9;
  const parenthesesKeyCode = 57;
  if (event.keyCode === tabKeyCode) {
    event.preventDefault();
    event.stopPropagation();
    const textarea = document.getElementById("large_text_field");
    const currTxt = textarea.value;
    const cursorPosition = textarea.selectionStart;
    textarea.value = currTxt.substring(0, cursorPosition) + '\t' + currTxt.substring(cursorPosition);
    textarea.selectionStart = cursorPosition + '\t'.length;
    textarea.selectionEnd = textarea.selectionStart;
  }
  if (event.keyCode === 57) {
    event.preventDefault();
    event.stopPropagation();
    const textarea = document.getElementById("large_text_field");
    const currTxt = textarea.value;
    const cursorPosition = textarea.selectionStart;
    textarea.value = currTxt.substring(0, cursorPosition) + '()' + currTxt.substring(cursorPosition);
    textarea.selectionStart = cursorPosition + 1;
    textarea.selectionEnd = textarea.selectionStart;
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
  document.getElementById('large_text_field').value = '';
  // const myjson = await res.json();
  // console.log(myjson);
}

//--------------------DB------------------------

async function pleaseForTheLoveOfGod(event) {
  event.preventDefault();
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
    const button = document.createElement('button');
    button.textContent = database;
    button.textContent = database;
    button.dataset.value = database;
    button.addEventListener('click', () => {
      selectCurrDB(button.textContent);
    });
    button.classList.add('db-button');
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
  document.querySelector(`button[data-value="${name}"].db-button`).classList.add('active');
  pleaseForTheLoveOfGod2()
}

function deselectAllButtons() {
  const buttons = document.getElementsByClassName('db-button');
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

