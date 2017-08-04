/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.bean;

import java.io.Serializable;
import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

import sys.imp.ClienteDAOImp;
import sys.dao.ClienteDAO;
import sys.model.AgmaePersona;


@ManagedBean
@ViewScoped
public class ClienteBean implements Serializable {

    private List<AgmaePersona> listaClientes;
    private AgmaePersona cliente;

    public ClienteBean() {
        cliente = new AgmaePersona();
        
    }

    public List<AgmaePersona> getListaClientes() {
        ClienteDAO cDao = new ClienteDAOImp();
        listaClientes = cDao.listarClientes();
        return listaClientes;
    }

    public void setListaClientes(List<AgmaePersona> listaClientes) {
        this.listaClientes = listaClientes;
    }

    public AgmaePersona getCliente() {
        return cliente;
    }

    public void setCliente(AgmaePersona cliente) {
        this.cliente = cliente;
    }


}
